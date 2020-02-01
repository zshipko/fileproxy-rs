use std::path::Path;

use futures_util::StreamExt;
use tokio::prelude::*;
use tokio::io::AsyncReadExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid URL")]
    ParseError(#[from] url::ParseError),

    #[error("I/O error")]
    IO(#[from] std::io::Error),

    #[error("Reqwest error")]
    Reqest(#[from] reqwest::Error),

    #[error("Request failed")]
    Status(u16),

}

pub struct Client {
    addr: String,
}

impl Client {
    pub fn new<S: Into<String>>(addr: S) -> Client {
        Client { addr: addr.into() }
    }

    pub fn url<S: AsRef<str>>(&self, key: S) -> Result<reqwest::Url, Error> {
        Ok(reqwest::Url::parse(&self.addr)?.join(key.as_ref())?)
    }

    pub async fn download_file<S: AsRef<str>, P: AsRef<Path>>(&self, key: S, filename: P) -> Result<(), Error> {
        let res = reqwest::get(self.url(key)?).await?;
        let mut stream = res.bytes_stream();

        let mut f = tokio::fs::File::create(filename).await?;

        while let Some(Ok(data)) = stream.next().await {
            f.write_all(&data).await?;
        }
        Ok(())
    }

    pub async fn upload_file<S: AsRef<str>, P: AsRef<Path>>(&self, key: S, filename: P) -> Result<(), Error> {
        let client = reqwest::Client::new();

        let mut f = tokio::fs::File::open(filename).await?;

        let mut data = Vec::new();
        f.read_to_end(&mut data).await?;

        let res = client.put(self.url(key)?).body(data).send().await?;
        let status = res.status();

        if status != 200 {
            return Err(Error::Status(status.as_u16()))
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
