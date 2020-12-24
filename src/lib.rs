use byteorder::{BigEndian, ByteOrder};
use std::collections::vec_deque::VecDeque;

// ProtoParser is a postgres protocol parser. It does not
// contain its own buffer. It only returns valid buffer ranges
// and the current postgres message type for the caller to
// understand the state of its own buffer stream. This way,
// no copying is required.
pub struct ProtoParser {
    // The current msg type (if there is one).
    current_msg_type: Option<char>,
    // The expected size of the current message.
    current_msg_length: usize,
    // Bytes read of the current message (might take multiple buffers).
    current_msg_bytes_read: usize,
}

impl ProtoParser {
    pub fn new() -> Self {
        Self {
            current_msg_type: None,
            current_msg_length: 0,
            current_msg_bytes_read: 0,
        }
    }

    // The caller is expected to use a buffer range that was not previously
    // notated by the response ProtoMessages. The only exception is when
    // a Complete message follows < 5 bytes of buffer (meaning, not enough
    // to parse the message type and message size of the next message).
    // Those remaining bytes will need to be sent again with the next buffer.
    #[inline]
    pub fn next(&mut self, buffer: &[u8], msgs: &mut VecDeque<ProtoMessage>) -> usize {
        let mut offset = 0;

        if buffer.len() < 5 {
            // Not enought data to read a msg type and msg size.
            return 0;
        }

        // Is this OK? Can I just remove the last 4 from the range since the offset
        // left with too few bytes will just exit anyways?
        // Update: I think so.
        while offset < buffer.len() - 5 {
            // Handle partial message.
            if self.current_msg_type.is_some() {
                let remaining = self.current_msg_length - self.current_msg_bytes_read;
                let bytes_to_read = std::cmp::min(buffer.len(), remaining);
                let remaining = remaining - bytes_to_read;

                // We can only complete a partial if remaining is 0 and offset is 0.
                // The first part of this buffer contains the rest of a previously
                // started message.
                if remaining == 0 && offset == 0 {
                    msgs.push_back(ProtoMessage::PartialComplete(
                        self.current_msg_type.expect("partial message state"),
                        bytes_to_read - 1,
                    ));

                    // Update the offset.
                    offset += bytes_to_read;

                    // Reset the current msg because the partial is complete.
                    self.msg_complete();
                    continue;
                } else {
                    msgs.push_back(ProtoMessage::Partial(
                        self.current_msg_type.expect("partial message state"),
                        offset,
                        offset + bytes_to_read - 1,
                    ));

                    // Update the offset.
                    offset += bytes_to_read;
                    self.current_msg_bytes_read += bytes_to_read;
                    continue;
                }
            }

            // Expect and handle new message.
            self.current_msg_type = Some(buffer[offset] as char);
            self.current_msg_length = BigEndian::read_i32(&buffer[offset + 1..offset + 5]) as usize;

            let remaining = self.current_msg_length - self.current_msg_bytes_read;
            let bytes_to_read = std::cmp::min(buffer.len() - offset, remaining);
            let remaining = remaining - bytes_to_read;
            self.current_msg_bytes_read += bytes_to_read;

            if remaining == 0 {
                msgs.push_back(ProtoMessage::Message(
                    self.current_msg_type.expect("full message found"),
                    offset,
                    offset + bytes_to_read - 1,
                ));

                self.msg_complete();
            } else {
                msgs.push_back(ProtoMessage::Partial(
                    self.current_msg_type.expect("full message found"),
                    offset,
                    offset + bytes_to_read - 1,
                ));
            }
            offset += bytes_to_read;
        }

        offset
    }

    fn msg_complete(&mut self) {
        self.current_msg_type = None;
        self.current_msg_length = 0;
        self.current_msg_bytes_read = 0;
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ProtoMessage {
    // This message starts and stops within the current buffer.
    Message(char, usize, usize),
    // This message starts but does not end within the current buffer.
    // The second usize is the known size of the buffer.
    Partial(char, usize, usize),
    // This message started in a previous buffer but is now complete.
    PartialComplete(char, usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_returns_empty_when_missing_data() {
        let packet = &[84, 0, 0, 0];
        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        assert_eq!(parser.next(packet, &mut msgs), 0);
        assert_eq!(msgs.len(), 0);
    }

    #[test]
    fn it_can_parse_a_partial_and_skip_insufficient_buffer() {
        #[rustfmt::skip]
        let packet = &[
            // Complete C tag.
            67, 0, 0, 0, 13, 83, 69, 76, 69, 67, 84, 32, 49,
 
            // Only 4-bytes of a second C tag message.
            67, 0, 0, 0,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.next(packet, &mut msgs);
        assert_eq!(n, packet.len() - 4);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], ProtoMessage::Message('C', 0, 12));
    }

    #[test]
    fn it_can_parse_a_partial_over_several_buffers() {
        // T tag split over 3 packets.
        let packet1 = &[84, 0, 0, 0, 29, 0, 1, 103, 117, 105, 100, 0];
        let packet2 = &[0, 1, 54, 55, 0, 2, 0, 0, 4, 19, 255];
        let packet3 = &[255, 0, 0, 0, 44, 0];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n1 = parser.next(packet1, &mut msgs);
        let n2 = parser.next(packet2, &mut msgs);
        let n3 = parser.next(packet3, &mut msgs);

        assert_eq!([n1, n2, n3], [packet1.len(), packet2.len(), packet3.len()]);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], ProtoMessage::Partial('T', 0, 11));
        assert_eq!(msgs[1], ProtoMessage::Partial('T', 0, 10));
        assert_eq!(msgs[2], ProtoMessage::PartialComplete('T', 5));
    }

    #[test]
    fn it_can_parse_multiple_complete_msgs_and_then_a_partial() {
        #[rustfmt::skip]
        let packet = &[
            // T tag
            84, 0, 0, 0, 29, 0, 1, 103, 117, 105, 100, 0, 0, 1, 54, 55, 0, 2, 0, 0, 4, 19, 255, 255,
            0, 0, 0, 44, 0,
    
            // D tag
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.next(packet, &mut msgs);

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], ProtoMessage::Message('T', 0, 28));
        assert_eq!(msgs[1], ProtoMessage::Partial('D', 29, packet.len() - 1));
    }

    #[test]
    fn it_can_parse_multiple_complete_msgs() {
        #[rustfmt::skip]
        let packet = &[
            // T tag
            84, 0, 0, 0, 29, 0, 1, 103, 117, 105, 100, 0, 0, 1, 54, 55, 0, 2, 0, 0, 4, 19, 255, 255,
            0, 0, 0, 44, 0,
    
            // D tag
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
            45, 48, 97, 48, 49, 45, 52, 54, 100, 101, 45, 57, 99, 53, 101, 45, 55, 55, 101, 102,
            55, 101, 101, 57, 101, 51, 101,
    
            // C tag
            67, 0, 0, 0, 13, 83, 69, 76, 69, 67, 84, 32, 49,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.next(packet, &mut msgs);

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], ProtoMessage::Message('T', 0, 28));
        assert_eq!(msgs[1], ProtoMessage::Message('D', 29, 78));
        assert_eq!(msgs[2], ProtoMessage::Message('C', 79, packet.len() - 1));
    }

    #[test]
    fn it_can_parse_a_partial_msg_and_later_complete_the_partial() {
        // This is a row description message with a column called "guid"
        let packet1 = &[
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
            45, 48, 97, 48, 49, 45, 52, 54,
        ];
        let packet2 = &[
            100, 101, 45, 57, 99, 53, 101, 45, 55, 55, 101, 102, 55, 101, 101, 57, 101, 51, 101,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.next(packet1, &mut msgs);
        assert_eq!(n, packet1.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs.pop_front().unwrap(),
            ProtoMessage::Partial('D', 0, packet1.len() - 1)
        );

        let n = parser.next(packet2, &mut msgs);
        assert_eq!(n, packet2.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0],
            ProtoMessage::PartialComplete('D', packet2.len() - 1)
        );
    }

    #[test]
    fn it_can_parse_a_complete_msg() {
        // This is a row description message with a column called "guid"
        let packet = &[
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
            45, 48, 97, 48, 49, 45, 52, 54, 100, 101, 45, 57, 99, 53, 101, 45, 55, 55, 101, 102,
            55, 101, 101, 57, 101, 51, 101,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.next(packet, &mut msgs);

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], ProtoMessage::Message('D', 0, packet.len() - 1));
    }
}
