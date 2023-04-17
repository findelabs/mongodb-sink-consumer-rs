use bson::Document;
use mongodb::options::Credential;
use mongodb::{options::ClientOptions, Client};

use crate::error::Error as RestError;
use crate::Args;

type Result<T> = std::result::Result<T, RestError>;

#[derive(Clone, Debug)]
pub struct MongoClient {
    pub database: String,
    pub collection: String,
    pub client: Client,
}

impl MongoClient {
    pub async fn init(args: Args) -> Result<Self> {
        let cred = Credential::builder()
            .source(Some("admin".to_string()))
            .username(Some(args.mongodb_username))
            .password(args.mongodb_password)
            .build();

        // Create mongodb client options
        let mut client_options = ClientOptions::parse(&args.mongodb_uri).await?;
        client_options.credential = Some(cred);
        client_options.app_name = Some("mongodb-rest-rs".to_string());

        // Create actual client
        let client = Client::with_options(client_options)?;

        if let Err(e) = client.list_database_names(None, None).await {
            panic!("{}", e);
        };

        Ok(Self {
            database: args.mongodb_database,
            collection: args.mongodb_collection,
            client,
        })
    }

    pub async fn insert_one(&self, doc: Document) -> Result<()> {
        // Log which collection this is accessing
        log::debug!(
            "\"Inserting doc into {}.{}\"",
            self.database,
            self.collection
        );

        // Generate handle on collection
        let collection = self
            .client
            .database(&self.database)
            .collection::<Document>(&self.collection);

        // Insert into MongoDB
        match collection.insert_one(doc, None).await {
            Ok(id) => {
                log::info!("\"Inserted doc with id: {}\"", id.inserted_id);
                metrics::increment_counter!("mongodb_sink_connector_mongodb_inserts");
                Ok(())
            }
            Err(e) => {
                log::error!("\"Error inserting into mongodb: {}\"", e);
                metrics::increment_counter!("mongodb_sink_connector_mongodb_insert_errors");
                return Err(e)?;
            }
        }
    }
}
