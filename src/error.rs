use std::fmt;

#[derive(Debug)]
pub enum Error {
    Mongo(mongodb::error::Error),
    Bson(bson::document::ValueAccessError),
    DeError(bson::de::Error),
    SerError(bson::ser::Error),
    SRCError(schema_registry_converter::error::SRCError),
    Hyper(hyper::Error),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Mongo(ref err) => write!(f, "{{\"error\": \"{}\"}}", err),
            Error::Bson(ref err) => write!(f, "{{\"error\": \"{}\"}}", err),
            Error::DeError(ref err) => write!(f, "{{\"error\": \"{}\"}}", err),
            Error::SerError(ref err) => write!(f, "{{\"error\": \"{}\"}}", err),
            Error::SRCError(ref err) => write!(f, "{{\"error\": \"{}\"}}", err),
            Error::Hyper(ref err) => write!(f, "{{\"error\": \"{}\"}}", err),
        }
    }
}

impl From<bson::document::ValueAccessError> for Error {
    fn from(err: bson::document::ValueAccessError) -> Error {
        Error::Bson(err)
    }
}

impl From<bson::de::Error> for Error {
    fn from(err: bson::de::Error) -> Error {
        Error::DeError(err)
    }
}

impl From<bson::ser::Error> for Error {
    fn from(err: bson::ser::Error) -> Error {
        Error::SerError(err)
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(err: mongodb::error::Error) -> Error {
        Error::Mongo(err)
    }
}

impl From<schema_registry_converter::error::SRCError> for Error {
    fn from(err: schema_registry_converter::error::SRCError) -> Error {
        Error::SRCError(err)
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Error {
        Error::Hyper(err)
    }
}
