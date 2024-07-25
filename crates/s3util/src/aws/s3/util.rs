use anyhow::anyhow;
use aws_sdk_s3::{operation::put_object::PutObjectOutput, primitives::ByteStream, Client as S3Client};

use crate::aws::util::client::BUILDER;
//use aws_smithy_http::{byte_stream::ByteStream, result::SdkError};


pub trait GetFile {
    fn get_file<T: AsRef<str>, T2: AsRef<str>>(&self, bucket: T, key: T2) -> Result<Vec<u8>, anyhow::Error>;
}

pub trait SendFile {
    fn send_file(&self, route: String, token: String, vec: Vec<u8>) -> Result<String, anyhow::Error>;
    fn upload_object(&self, body: Vec<u8>, dest_bucket_name: &str, dest_key: &str) -> Result<PutObjectOutput, anyhow::Error>;
}

pub trait ListObjects {
    //async fn list_objects_with_ext<P: AsRef<str>>(&self, bucket: P, prefix: P, ext: P) -> Result<Vec<String>, anyhow::Error>;
    /// ext: path의 끝문자열로 "/" 로 지정할 경우 디렉토리 형식만 반환한다. 
    /// .parquet 은 parquet 파일 목록만 반환 
    /// None 인 경우 모든 항목 반환. / 포함
    fn list_all_objects<P: AsRef<str>>(&self, bucket: P, prefix: P, ext: Option<P>) -> Result<Vec<String>, anyhow::Error>;
}

impl GetFile for S3Client {
    fn get_file<T: AsRef<str>, T2: AsRef<str>>(&self, bucket: T, key: T2) -> Result<Vec<u8>, anyhow::Error> {
        let result = BUILDER.block_on(async {
            let f = self
                .get_object()
                .bucket(bucket.as_ref())
                .key(key.as_ref())
                .send().await?;
            let buf = f.body.collect().await?;
            Ok(buf.to_vec())
        });
        
        // let mut writer = bytebuffer::ByteBuffer::new();
        // while let Some(bytes) = f.body.try_next().await.unwrap() {
        //     println!("{}", format!("{} {}", key.as_ref(), bytes.len()));
        //     let _ = writer.write(&bytes);
        // }
        
        result
    }
}

impl SendFile for S3Client {
    fn send_file(&self, route: String, token: String, vec: Vec<u8>) -> Result<String, anyhow::Error> {
        tracing::info!("send file route {}, token {}, length {}", route, token, vec.len());
        let write = BUILDER.block_on(async {
            let bytes = ByteStream::from(vec);

            let write = self
                .write_get_object_response()
                .request_route(route)
                .request_token(token)
                .status_code(200)
                .body(bytes)
                .send()
                .await;
            write
        });

        if write.is_err() {
            //let sdk_error = write.err().unwrap();
            //check_error(sdk_error);
            Err(anyhow::anyhow!("WriteGetObjectResponse creation error"))
        } else {
            Ok("File sent.".to_string())
        }
    }

    fn upload_object(&self, body: Vec<u8>, dest_bucket_name: &str, dest_key: &str) -> Result<PutObjectOutput, anyhow::Error> {
        //let bytes = ByteStream::from(body);
        let result = BUILDER.block_on(async {
            let result = self
                .put_object()
                .bucket(dest_bucket_name)
                .key(dest_key)
                .body(body.into())
                .send()
                .await;
            result
        });
        result.map_err(|e|anyhow!(e))
    }
}

impl ListObjects for S3Client {
    // async fn list_objects_with_ext<P: AsRef<str>>(&self, bucket: P, prefix: P, ext: P) -> Result<Vec<String>, anyhow::Error> {
    //     // let response = self
    //     //     .list_objects_v2()
    //     //     .bucket(bucket.as_ref())
    //     //     .prefix(prefix.as_ref())
    //     //     .max_keys(count)
    //     //     .into_paginator()
    //     //     .send();
    //     // //let v = response.try_collect().await?;
    //     // let r:Vec<_> = response.try_collect().await?.into_iter()
    //     //     .flat_map(|output| output.contents.unwrap().into_iter().filter(|o|o.key().unwrap().ends_with(ext.as_ref()))).collect();
    //     //let r2: Vec<_> = r.iter().map(|o|o.key().unwrap().to_owned()).collect();
    //     //println!("{:?}", r);

    //     let r:Vec<_> = self.list_all_objects(bucket, prefix).await?.iter().filter(|path| path.ends_with(".pgp")).collect();
    //     Ok(r)
    // }

    fn list_all_objects<P: AsRef<str>>(&self, bucket: P, prefix: P, ext: Option<P>) -> Result<Vec<String>, anyhow::Error> {
        let result = BUILDER.block_on(async {
            let mut continuation_token = None;
            let mut object_keys = Vec::new();

            loop {
                let resp = self
                    .list_objects_v2()
                    .bucket(bucket.as_ref())
                    .prefix(prefix.as_ref())
                    .set_continuation_token(continuation_token.clone())
                    .send()
                    .await?;
        
                if let Some(contents) = resp.contents {
                    for object in contents {
                        if let Some(key) = object.key {
                            if let Some(ref ext) = ext {
                                if key.ends_with(ext.as_ref()) {    // 확장자가 일치하지 않는 경우 비어있는 object_keys 반환
                                    object_keys.push(key);
                                }
                            } else {
                                object_keys.push(key);
                            }
                            
                        }
                    }
                }
        
                if resp.is_truncated.unwrap() {
                    continuation_token = resp.next_continuation_token;
                } else {
                    break;
                }
            }
            Ok(object_keys)
        });
    
        result
    }
}
