use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

#[derive(Debug, Clone)]
pub enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespValue>),
    Map(Vec<(RespValue, RespValue)>),
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Option<RespValue>, Box<dyn std::error::Error>>
where
    R: AsyncBufRead + AsyncReadExt + Unpin,
{
    let mut first = [0_u8; 1];
    match reader.read_exact(&mut first).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let frame = match first[0] {
        b'*' => {
            let count = read_len(reader).await?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let mut prefix = [0_u8; 1];
                reader.read_exact(&mut prefix).await?;
                match prefix[0] {
                    b'$' => {
                        let len = read_signed_len(reader).await?;
                        if len < 0 {
                            values.push(RespValue::Bulk(None));
                        } else {
                            let bulk = read_bulk(reader, len as usize).await?;
                            values.push(RespValue::Bulk(Some(bulk)));
                        }
                    }
                    b'+' => values.push(RespValue::Simple(read_line(reader).await?)),
                    b':' => {
                        let n = read_line(reader).await?.parse::<i64>()?;
                        values.push(RespValue::Integer(n));
                    }
                    _ => return Err("unsupported RESP array element".into()),
                }
            }
            RespValue::Array(values)
        }
        b'+' => RespValue::Simple(read_line(reader).await?),
        b'$' => {
            let len = read_signed_len(reader).await?;
            if len < 0 {
                RespValue::Bulk(None)
            } else {
                RespValue::Bulk(Some(read_bulk(reader, len as usize).await?))
            }
        }
        b':' => RespValue::Integer(read_line(reader).await?.parse::<i64>()?),
        _ => return Err("unsupported RESP type".into()),
    };

    Ok(Some(frame))
}

pub fn encode(value: RespValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    encode_into(&mut out, value);
    out
}

fn encode_into(dst: &mut Vec<u8>, value: RespValue) {
    match value {
        RespValue::Simple(v) => {
            dst.push(b'+');
            dst.extend_from_slice(v.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Error(v) => {
            dst.push(b'-');
            dst.extend_from_slice(v.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(v) => {
            dst.push(b':');
            dst.extend_from_slice(v.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Bulk(None) => dst.extend_from_slice(b"$-1\r\n"),
        RespValue::Bulk(Some(v)) => {
            dst.push(b'$');
            dst.extend_from_slice(v.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&v);
            dst.extend_from_slice(b"\r\n");
        }
        RespValue::Array(values) => {
            dst.push(b'*');
            dst.extend_from_slice(values.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for value in values {
                encode_into(dst, value);
            }
        }
        RespValue::Map(entries) => {
            dst.push(b'%');
            dst.extend_from_slice(entries.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for (k, v) in entries {
                encode_into(dst, k);
                encode_into(dst, v);
            }
        }
    }
}

pub fn frame_to_args(frame: RespValue) -> Result<Vec<Vec<u8>>, String> {
    match frame {
        RespValue::Array(items) => {
            let mut args = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RespValue::Bulk(Some(v)) => args.push(v),
                    RespValue::Simple(v) => args.push(v.into_bytes()),
                    _ => return Err("ERR command must be bulk-string array".to_string()),
                }
            }
            Ok(args)
        }
        _ => Err("ERR expected array command frame".to_string()),
    }
}

async fn read_line<R>(reader: &mut R) -> Result<String, Box<dyn std::error::Error>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).await?;
    if line.len() < 2 || line[line.len() - 2] != b'\r' {
        return Err("invalid RESP line ending".into());
    }
    line.truncate(line.len() - 2);
    Ok(String::from_utf8(line)?)
}

async fn read_len<R>(reader: &mut R) -> Result<usize, Box<dyn std::error::Error>>
where
    R: AsyncBufRead + Unpin,
{
    Ok(read_line(reader).await?.parse::<usize>()?)
}

async fn read_signed_len<R>(reader: &mut R) -> Result<i64, Box<dyn std::error::Error>>
where
    R: AsyncBufRead + Unpin,
{
    Ok(read_line(reader).await?.parse::<i64>()?)
}

async fn read_bulk<R>(reader: &mut R, len: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>>
where
    R: AsyncBufRead + Unpin,
{
    let mut payload = vec![0_u8; len + 2];
    reader.read_exact(&mut payload).await?;
    if payload[len] != b'\r' || payload[len + 1] != b'\n' {
        return Err("invalid RESP bulk ending".into());
    }
    payload.truncate(len);
    Ok(payload)
}
