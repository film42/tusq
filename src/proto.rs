use byteorder::{BigEndian, ByteOrder};
use std::collections::BTreeMap;
use std::collections::VecDeque;

// TODO: How do we make this a bit cleaner? Constants?
pub mod messages {
    use byteorder::{BigEndian, ByteOrder};

    pub fn password_cleartext(password: &str) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(b'p');
        // Set range aside for size at the end.
        msg.extend_from_slice(&[0, 0, 0, 0]);
        msg.extend_from_slice(&password.as_bytes());
        msg.push(0);

        let msg_proto_size = msg.len() - 1;
        BigEndian::write_i32(&mut msg[1..5], msg_proto_size as i32);
        msg
    }

    // concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
    pub fn password_md5(username: &str, password: &str, salt: &[u8]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(b'p');
        // Set range aside for size at the end.
        msg.extend_from_slice(&[0, 0, 0, 0]);
        // concat(password, username)
        let userpass = format!("{}{}", password, username);
        // md5(ABOVE)
        let md5 = format!("{:x}", md5::compute(userpass.as_bytes()));
        // concat(ABOVE, random-salt)
        let md5: Vec<_> = md5.bytes().chain(salt.iter().copied()).collect();
        // concat('md5', md5(ABOVE))
        let md5 = format!("md5{:x}", md5::compute(&md5));
        msg.extend_from_slice(&md5.as_bytes());
        msg.push(0);

        let msg_proto_size = msg.len() - 1;
        BigEndian::write_i32(&mut msg[1..5], msg_proto_size as i32);
        msg
    }

    pub fn auth_ok() -> Vec<u8> {
        let mut msg = [0; 9];
        msg[0] = b'R';
        BigEndian::write_i32(&mut msg[1..5], 8);
        BigEndian::write_i32(&mut msg[5..9], 0);
        msg.into()
    }

    pub fn ready_for_query() -> Vec<u8> {
        let mut msg = [0; 6];
        msg[0] = b'Z';
        BigEndian::write_i32(&mut msg[1..5], 5);
        msg[5] = b'I';
        msg.into()
    }

    pub fn server_parameter(key: &str, value: &str) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(b'S');
        // Set range aside for size at the end.
        msg.extend_from_slice(&[0, 0, 0, 0]);
        msg.extend_from_slice(key.as_bytes());
        msg.push(0);
        msg.extend_from_slice(value.as_bytes());
        msg.push(0);
        let msg_proto_size = msg.len() - 1;
        BigEndian::write_i32(&mut msg[1..5], msg_proto_size as i32);
        msg
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn it_can_create_a_cleartext_password_response() {
            let password = "123456";
            let expected = &[112, 0, 0, 0, 11, 49, 50, 51, 52, 53, 54, 0];
            assert_eq!(&password_cleartext(password), expected);
        }

        #[test]
        fn it_can_create_an_md5_password_response() {
            let user = "testuser";
            let password = "123456";
            let salt = &[0x17, 0xF5, 0x9E, 0x3E];

            // Underlying md5 password: md5c7342a0451b0de1a27c3e7e31776792e
            let expected = &[
                112, 0, 0, 0, 40, 109, 100, 53, 99, 55, 51, 52, 50, 97, 48, 52, 53, 49, 98, 48,
                100, 101, 49, 97, 50, 55, 99, 51, 101, 55, 101, 51, 49, 55, 55, 54, 55, 57, 50,
                101, 0,
            ];
            assert_eq!(&password_md5(user, password, salt), expected);
        }
    }
}

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
    // Startup message if one is being parsed.
    current_startup_message: Option<StartupMessage>,
    current_startup_parameter_key: Option<String>,
    current_startup_parameter_value: Option<String>,
}

const CANCEL_REQUEST_VERSION: i32 = 80877102;
const SSL_REQUEST_VERSION: i32 = 80877103;

impl ProtoParser {
    pub fn new() -> Self {
        Self {
            current_msg_type: None,
            current_msg_length: 0,
            current_msg_bytes_read: 0,
            current_startup_message: None,
            current_startup_parameter_key: None,
            current_startup_parameter_value: None,
        }
    }

    #[inline]
    pub fn msg_size(buffer: &[u8]) -> Option<usize> {
        if buffer.len() < 4 {
            return None;
        }
        Some(BigEndian::read_i32(&buffer[0..4]) as usize)
    }

    // This will parse a StartupMessage or SSLRequest using one or more buffers.
    // Unlike the `parse` method, this will copy data in that buffer to create
    // a shareable startup message.
    pub fn parse_startup(
        &mut self,
        buffer: &[u8],
    ) -> anyhow::Result<(usize, Option<ProtoStartup>)> {
        let mut offset = 0;

        // If we don't have a current startup message, be sure to parse
        // the size and allocate a new struct.
        if self.current_startup_message.is_none() {
            // First 4 bytes tell us the size of the startup message.
            if buffer.len() < 4 {
                return Ok((0, None));
            }
            self.current_msg_length = BigEndian::read_i32(&buffer[0..4]) as usize;
            self.current_startup_message = Some(StartupMessage::new());
            offset += 4;
            self.current_msg_bytes_read += 4;
        }

        if let Some(ref mut startup_message) = self.current_startup_message {
            // Check if we need to parse the protocol version.
            if self.current_msg_bytes_read < 8 {
                let bytes_to_read = std::cmp::min(4, buffer.len() - offset);
                if bytes_to_read < 4 {
                    return Ok((offset, None));
                }

                startup_message.protocol_version = BigEndian::read_i32(&buffer[offset..offset + 4]);
                offset += 4;
                self.current_msg_bytes_read += 4;
            }

            // Detect if this is a Cancel Request
            if startup_message.protocol_version == CANCEL_REQUEST_VERSION
                && self.current_msg_length == 16
            {
                self.msg_complete();
                self.current_startup_message = None;
                return Ok((16, Some(ProtoStartup::CancelRequest)));
            }

            // Detect if this is an SSL Request
            if startup_message.protocol_version == SSL_REQUEST_VERSION
                && self.current_msg_length == 8
            {
                self.msg_complete();
                self.current_startup_message = None;
                return Ok((8, Some(ProtoStartup::SSLRequest)));
            }

            loop {
                // Check for parameter termination.
                if buffer[offset] == 0 {
                    // Reset counters.
                    self.msg_complete();
                    return Ok((
                        offset + 1,
                        self.current_startup_message
                            .take()
                            .map(ProtoStartup::Message),
                    ));
                }

                // Otherwise we are parsing parameters.
                // TODO: There is some state tracking so we can essentially use 1-byte
                // buffers after reading the first 4-byte msg size, but I don't feel
                // like writing that at the moment.

                // Parse the key...
                if self.current_startup_parameter_key.is_none() {
                    let pos = memchr::memchr(0, &buffer[offset..])
                        .expect("no support for partial cstr reads for now");

                    // Parse the entire valid cstr and move forward.
                    let cstr = &buffer[offset..offset + pos];
                    self.current_startup_parameter_key = Some(
                        std::str::from_utf8(&cstr)
                            .expect("todo: add error handling")
                            .into(),
                    );

                    offset += pos + 1;
                    self.current_msg_bytes_read += pos + 1;
                }

                // Parse the value...
                if self.current_startup_parameter_value.is_none() {
                    let pos = memchr::memchr(0, &buffer[offset..])
                        .expect("no support for partial cstr reads for now");

                    // Parse the entire valid cstr and move forward.
                    let cstr = &buffer[offset..offset + pos];
                    self.current_startup_parameter_value = Some(
                        std::str::from_utf8(&cstr)
                            .expect("todo: add error handling")
                            .into(),
                    );

                    offset += pos + 1;
                    self.current_msg_bytes_read += pos + 1;
                }

                // Store parameter and reset startup param variables.
                startup_message.parameters.insert(
                    self.current_startup_parameter_key
                        .take()
                        .expect("checked above"),
                    self.current_startup_parameter_value
                        .take()
                        .expect("checked above"),
                );
            }
        }

        // Then begin parsing with available buffer.
        Ok((0, None))
    }

    // The caller is expected to use a buffer range that was not previously
    // notated by the response ProtoMessages. The only exception is when
    // a Complete message follows < 5 bytes of buffer (meaning, not enough
    // to parse the message type and message size of the next message).
    // Those remaining bytes will need to be sent again with the next buffer.
    #[inline]
    pub fn parse(
        &mut self,
        buffer: &[u8],
        msgs: &mut VecDeque<ProtoMessage>,
    ) -> anyhow::Result<usize> {
        let mut offset = 0;

        if buffer.len() < 5 {
            // Not enought data to read a msg type and msg size.
            return Ok(0);
        }

        // Is this OK? Can I just remove the last 4 from the range since the offset
        // left with too few bytes will just exit anyways?
        // Update: I think so.
        while offset < buffer.len() - 4 {
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
            offset += 1;
            self.current_msg_length = BigEndian::read_i32(&buffer[offset..offset + 4]) as usize;

            let remaining = self.current_msg_length - self.current_msg_bytes_read;
            let bytes_to_read = std::cmp::min(buffer.len() - offset, remaining);

            let remaining = remaining - bytes_to_read;
            self.current_msg_bytes_read += bytes_to_read;

            if remaining == 0 {
                msgs.push_back(ProtoMessage::Message(
                    self.current_msg_type.expect("full message found"),
                    offset - 1,
                    offset + bytes_to_read - 1,
                ));

                self.msg_complete();
            } else {
                msgs.push_back(ProtoMessage::Partial(
                    self.current_msg_type.expect("full message found"),
                    offset - 1,
                    offset + bytes_to_read - 1,
                ));
            }

            offset += bytes_to_read;
        }

        Ok(offset)
    }

    fn msg_complete(&mut self) {
        self.current_msg_type = None;
        self.current_msg_length = 0;
        self.current_msg_bytes_read = 0;
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ProtoAuth<'a> {
    AuthOk,
    AuthMD5Password(&'a [u8]),
    AuthCleartextPassword,
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

impl ProtoMessage {
    // TODO: Make this work with a Partial + PartialComplete.
    pub fn error_message(&self, buffer: &[u8]) -> anyhow::Result<Option<String>> {
        if let ProtoMessage::Message('E', start, end) = self {
            if end - start < 6 {
                return Ok(None);
            }
            if buffer[start + 6] == 0 {
                // Empty string error.
                return Ok(Some("".to_string()));
            }

            // Convert the buffer error string into a String.
            let msg = std::str::from_utf8(&buffer[start + 7..*end])?.to_string();
            return Ok(Some(msg));
        }
        Ok(None)
    }

    // TODO: Make this work with a Partial + PartialComplete.
    pub fn authentication_type<'a>(&self, buffer: &'a [u8]) -> Option<ProtoAuth<'a>> {
        if let ProtoMessage::Message('R', start, end) = self {
            if end - start < 8 {
                return None;
            }
            let auth_type = BigEndian::read_i32(&buffer[start + 5..start + 10]);

            return match auth_type {
                0 => Some(ProtoAuth::AuthOk),
                3 => Some(ProtoAuth::AuthCleartextPassword),
                5 => {
                    // Check for additional buffer size.
                    if buffer.len() <= start + 13 {
                        return None;
                    }
                    Some(ProtoAuth::AuthMD5Password(&buffer[start + 9..start + 13]))
                }
                _ => {
                    log::trace!("Missing authentication type code: {}", auth_type);
                    None
                }
            };
        }
        None
    }

    // Pull the txn type from a ready for query message.
    // TODO: Make this work with a Partial + PartialComplete.
    pub fn transaction_type(&self, buffer: &[u8]) -> Option<char> {
        if let ProtoMessage::Message('Z', start, end) = self {
            if end - start == 5 {
                return Some(buffer[start + 5] as char);
            }
        }
        None
    }

    pub fn server_parameter(&self, buffer: &[u8]) -> Option<(String, String)> {
        if let ProtoMessage::Message('S', start, _end) = self {
            // TODO: Make this safer. For now this assumes the message is valid.
            // Skip the first 5 bytes for [char, i32] and move on to String.
            let mut offset = 5 + start;

            let key_end = memchr::memchr(0, &buffer[offset..])
                .expect("no support for partial cstr reads for now");
            let key = std::str::from_utf8(&buffer[offset..offset + key_end])
                .expect("valid utf8")
                .into();

            // Update offset.
            offset += key_end + 1;

            let value_end = memchr::memchr(0, &buffer[offset..])
                .expect("no support for partial cstr reads for now");
            let value = std::str::from_utf8(&buffer[offset..offset + value_end])
                .expect("valid utf8")
                .into();

            return Some((key, value));
        }
        None
    }

    pub fn is_complete(&self) -> bool {
        if let ProtoMessage::Message(_, _, _) = self {
            true
        } else {
            false
        }
    }

    pub fn is_partial(&self) -> bool {
        !self.is_complete()
    }

    pub fn msg_type(&self) -> char {
        *match self {
            ProtoMessage::Message(msg_type, _, _) => msg_type,
            ProtoMessage::Partial(msg_type, _, _) => msg_type,
            ProtoMessage::PartialComplete(msg_type, _) => msg_type,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ProtoStartupMessage {
    Partial(usize, usize),
    PartialComplete(usize),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ProtoStartup {
    Message(StartupMessage),
    SSLRequest,
    CancelRequest,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StartupMessage {
    pub protocol_version: i32,
    pub parameters: BTreeMap<String, String>,
}

impl StartupMessage {
    pub fn new() -> Self {
        Self {
            protocol_version: 0,
            parameters: BTreeMap::new(),
        }
    }

    pub fn database_name(&self) -> Option<String> {
        match self.parameters.get("database") {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    // Convert the startup message back to proto bytes.
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut msg = Vec::new();
        // Reserve for size.
        msg.extend_from_slice(&[0, 0, 0, 0]);

        // Protocol version.
        msg.extend_from_slice(&[0, 0, 0, 0]);
        BigEndian::write_i32(&mut msg[4..8], self.protocol_version);

        // Key/value params.
        for (key, value) in self.parameters.iter() {
            msg.extend_from_slice(&key.as_bytes());
            msg.push(0);
            msg.extend_from_slice(&value.as_bytes());
            msg.push(0);
        }

        // Terminating null byte.
        msg.push(0);

        // Update size.
        let msg_size = msg.len() as i32;
        BigEndian::write_i32(&mut msg[0..4], msg_size);

        msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected_startup_message() -> StartupMessage {
        let mut expected = StartupMessage::new();
        expected.protocol_version = 196608;
        expected
            .parameters
            .insert("application_name".into(), "psql".into());
        expected
            .parameters
            .insert("client_encoding".into(), "UTF8".into());
        expected
            .parameters
            .insert("database".into(), "dispatch_development".into());
        expected.parameters.insert("user".into(), "postgres".into());
        expected
    }

    #[test]
    fn it_can_parse_a_complete_startup_message() {
        let startup_message_packet = &[
            0, 0, 0, 96, 0, 3, 0, 0, 117, 115, 101, 114, 0, 112, 111, 115, 116, 103, 114, 101, 115,
            0, 100, 97, 116, 97, 98, 97, 115, 101, 0, 100, 105, 115, 112, 97, 116, 99, 104, 95,
            100, 101, 118, 101, 108, 111, 112, 109, 101, 110, 116, 0, 97, 112, 112, 108, 105, 99,
            97, 116, 105, 111, 110, 95, 110, 97, 109, 101, 0, 112, 115, 113, 108, 0, 99, 108, 105,
            101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0, 85, 84, 70, 56, 0, 0,
        ];

        let mut parser = ProtoParser::new();

        let (n, startup_message) = parser.parse_startup(startup_message_packet).unwrap();
        assert_eq!(n, startup_message_packet.len());
        assert_eq!(
            startup_message.unwrap(),
            ProtoStartup::Message(expected_startup_message())
        );
    }

    #[test]
    fn it_returns_empty_when_missing_data() {
        let packet = &[84, 0, 0, 0];
        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        assert_eq!(parser.parse(packet, &mut msgs).unwrap(), 0);
        assert_eq!(msgs.len(), 0);
    }

    #[test]
    fn it_can_parse_a_partial_and_skip_insufficient_buffer() {
        #[rustfmt::skip]
        let packet = &[
            // Complete C tag.
            67, 0, 0, 0, 13, 83, 69, 76, 69, 67, 84, 32, 49, 0,
    
            // Only 4-bytes of a second C tag message.
            67, 0, 0, 0,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(packet, &mut msgs).unwrap();
        assert_eq!(n, packet.len() - 4);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], ProtoMessage::Message('C', 0, 13));
    }

    #[test]
    fn it_can_parse_a_partial_over_several_buffers() {
        // T tag split over 3 packets.
        let packet1 = &[84, 0, 0, 0, 29, 0, 1, 103, 117, 105, 100, 0];
        let packet2 = &[0, 1, 54, 55, 0, 2, 0, 0, 4, 19, 255];
        let packet3 = &[255, 0, 0, 0, 44, 0, 0];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n1 = parser.parse(packet1, &mut msgs).unwrap();
        let n2 = parser.parse(packet2, &mut msgs).unwrap();
        let n3 = parser.parse(packet3, &mut msgs).unwrap();

        assert_eq!([n1, n2, n3], [packet1.len(), packet2.len(), packet3.len()]);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], ProtoMessage::Partial('T', 0, 11));
        assert_eq!(msgs[1], ProtoMessage::Partial('T', 0, 10));
        assert_eq!(msgs[2], ProtoMessage::PartialComplete('T', 6));
    }

    #[test]
    fn it_can_parse_multiple_complete_msgs_and_then_a_partial() {
        #[rustfmt::skip]
        let packet = &[
            // T tag
            84, 0, 0, 0, 29, 0, 1, 103, 117, 105, 100, 0, 0, 1, 54, 55, 0, 2, 0, 0, 4, 19, 255, 255,
            0, 0, 0, 44, 0, 0,

            // D tag
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(packet, &mut msgs).unwrap();

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0], ProtoMessage::Message('T', 0, 29));
        assert_eq!(msgs[1], ProtoMessage::Partial('D', 30, packet.len() - 1));
    }

    #[test]
    fn it_can_parse_auth_ok() {
        #[rustfmt::skip]
        let packet = messages::auth_ok();

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(&packet, &mut msgs).unwrap();

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], ProtoMessage::Message('R', 0, 8));
    }

    #[test]
    fn it_can_parse_multiple_complete_msgs() {
        #[rustfmt::skip]
        let packet = &[
            // T tag
            84, 0, 0, 0, 29, 0, 1, 103, 117, 105, 100, 0, 0, 1, 54, 55, 0, 2, 0, 0, 4, 19, 255, 255,
            0, 0, 0, 44, 0, 0,

            // D tag
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
            45, 48, 97, 48, 49, 45, 52, 54, 100, 101, 45, 57, 99, 53, 101, 45, 55, 55, 101, 102,
            55, 101, 101, 57, 101, 51, 101, 52,

            // C tag
            67, 0, 0, 0, 13, 83, 69, 76, 69, 67, 84, 32, 49, 0,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(packet, &mut msgs).unwrap();

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0], ProtoMessage::Message('T', 0, 29));
        assert_eq!(msgs[1], ProtoMessage::Message('D', 30, 80));
        assert_eq!(msgs[2], ProtoMessage::Message('C', 81, packet.len() - 1));
    }

    #[test]
    fn it_can_parse_a_partial_msg_and_later_complete_the_partial() {
        // This is a row description message with a column called "guid"
        let packet1 = &[
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
            45, 48, 97, 48, 49, 45, 52, 54,
        ];
        let packet2 = &[
            100, 101, 45, 57, 99, 53, 101, 45, 55, 55, 101, 102, 55, 101, 101, 57, 101, 51, 101, 52,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(packet1, &mut msgs).unwrap();
        assert_eq!(n, packet1.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs.pop_front().unwrap(),
            ProtoMessage::Partial('D', 0, packet1.len() - 1)
        );

        let n = parser.parse(packet2, &mut msgs).unwrap();
        assert_eq!(n, packet2.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(
            msgs[0],
            ProtoMessage::PartialComplete('D', packet2.len() - 1)
        );
    }

    #[test]
    fn it_can_parse_a_complete_msg() {
        // This is a data row message with a column called "guid"
        let packet = &[
            68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 83, 72, 82, 45, 100, 54, 52, 97, 100, 99, 101, 55,
            45, 48, 97, 48, 49, 45, 52, 54, 100, 101, 45, 57, 99, 53, 101, 45, 55, 55, 101, 102,
            55, 101, 101, 57, 101, 51, 101, 52,
        ];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(packet, &mut msgs).unwrap();

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], ProtoMessage::Message('D', 0, packet.len() - 1));
    }

    #[test]
    fn it_can_parse_a_sync_msg() {
        let packet = &[b'S', 0, 0, 0, 4];

        let mut msgs = VecDeque::new();
        let mut parser = ProtoParser::new();
        let n = parser.parse(packet, &mut msgs).unwrap();

        assert_eq!(n, packet.len());
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], ProtoMessage::Message('S', 0, packet.len() - 1));
    }
}
