use super::{MalformedProtobuf, PreflightError, WorkBudget};

pub(super) const MAX_PROTOBUF_NESTING: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WireType {
    Varint = 0,
    Fixed64 = 1,
    LengthDelimited = 2,
    StartGroup = 3,
    EndGroup = 4,
    Fixed32 = 5,
}

impl WireType {
    fn from_key(value: u64) -> Result<Self, MalformedProtobuf> {
        match value & 7 {
            0 => Ok(Self::Varint),
            1 => Ok(Self::Fixed64),
            2 => Ok(Self::LengthDelimited),
            3 => Ok(Self::StartGroup),
            4 => Ok(Self::EndGroup),
            5 => Ok(Self::Fixed32),
            _ => Err(MalformedProtobuf::new(
                "protobuf field has an invalid wire type",
            )),
        }
    }
}

pub(super) struct Cursor<'a> {
    remaining: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    pub(super) fn key(&mut self) -> Result<(u32, WireType), MalformedProtobuf> {
        let key = self.varint()?;
        let field = key >> 3;
        if field == 0 || field > 0x1fff_ffff {
            return Err(MalformedProtobuf::new(
                "protobuf field number is out of range",
            ));
        }
        Ok((field as u32, WireType::from_key(key)?))
    }

    pub(super) fn varint(&mut self) -> Result<u64, MalformedProtobuf> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = self.take_byte()?;
            if index == 9 && byte > 1 {
                return Err(MalformedProtobuf::new("protobuf varint overflows u64"));
            }
            value |= u64::from(byte & 0x7f) << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(MalformedProtobuf::new("protobuf varint is unterminated"))
    }

    pub(super) fn fixed32(&mut self) -> Result<(), MalformedProtobuf> {
        self.take(4).map(|_| ())
    }

    pub(super) fn fixed64(&mut self) -> Result<(), MalformedProtobuf> {
        self.take(8).map(|_| ())
    }

    pub(super) fn length_delimited(&mut self) -> Result<&'a [u8], MalformedProtobuf> {
        let length = usize::try_from(self.varint()?)
            .map_err(|_| MalformedProtobuf::new("protobuf length does not fit in memory"))?;
        self.take(length)
    }

    pub(super) fn skip_unknown(
        &mut self,
        field: u32,
        wire: WireType,
        depth: usize,
        work: &mut WorkBudget,
    ) -> Result<(), PreflightError> {
        // Prost checks its recursion context before dispatching every unknown wire type,
        // including scalars at the deepest permitted known-message level.
        if depth >= MAX_PROTOBUF_NESTING {
            return Err(
                MalformedProtobuf::new("protobuf nesting exceeds the decoder limit").into(),
            );
        }
        match wire {
            WireType::Varint => self.varint().map(|_| ()).map_err(Into::into),
            WireType::Fixed64 => self.fixed64().map_err(Into::into),
            WireType::LengthDelimited => self.length_delimited().map(|_| ()).map_err(Into::into),
            WireType::StartGroup => {
                work.charge(1)?;
                self.skip_group(field, depth + 1, work)
            }
            WireType::EndGroup => {
                Err(MalformedProtobuf::new("protobuf has a stray end group").into())
            }
            WireType::Fixed32 => self.fixed32().map_err(Into::into),
        }
    }

    fn skip_group(
        &mut self,
        expected: u32,
        depth: usize,
        work: &mut WorkBudget,
    ) -> Result<(), PreflightError> {
        if depth > MAX_PROTOBUF_NESTING {
            return Err(
                MalformedProtobuf::new("protobuf nesting exceeds the decoder limit").into(),
            );
        }
        while !self.is_empty() {
            let (field, wire) = self.key()?;
            work.charge(1)?;
            if wire == WireType::EndGroup {
                return if field == expected {
                    Ok(())
                } else {
                    Err(MalformedProtobuf::new("protobuf group end tag does not match").into())
                };
            }
            self.skip_unknown(field, wire, depth, work)?;
        }
        Err(MalformedProtobuf::new("protobuf group is unterminated").into())
    }

    fn take_byte(&mut self) -> Result<u8, MalformedProtobuf> {
        let Some((&byte, remaining)) = self.remaining.split_first() else {
            return Err(MalformedProtobuf::new("protobuf value is truncated"));
        };
        self.remaining = remaining;
        Ok(byte)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], MalformedProtobuf> {
        if length > self.remaining.len() {
            return Err(MalformedProtobuf::new("protobuf value is truncated"));
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{Cursor, WorkBudget};

    #[test]
    fn unknown_groups_are_checked_without_allocating_a_stack() {
        let mut cursor = Cursor::new(&[0x0b, 0x13, 0x18, 0x01, 0x14, 0x0c]);
        let (field, wire) = cursor.key().unwrap();
        let mut work = WorkBudget::new(100);
        cursor.skip_unknown(field, wire, 0, &mut work).unwrap();
        assert!(cursor.is_empty());

        let mut mismatched = Cursor::new(&[0x0b, 0x14]);
        let (field, wire) = mismatched.key().unwrap();
        assert!(mismatched.skip_unknown(field, wire, 0, &mut work).is_err());
    }
}
