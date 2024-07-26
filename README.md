# ddlview

<pre>
<code>
Usage: ddlviewer [OPTIONS] <COMMAND>

Commands:
  schema  
  head    
  sql     
  help    Print this message or the help of the given subcommand(s)

Options:
  -p, --profile <PROFILE>               profile name
  -i, --id <AWS_ACCESS_KEY_ID>          aws_access_key_id
  -k, --secret <AWS_SECRET_ACCESS_KEY>  aws_secret_access_key
  -r, --region <REGION>                 [default: ap-northeast-2]
  -h, --help                            Print help
  -V, --version                         Print version
</code>
</pre>

### schema 조회
<pre>
<code>
Usage: ddlviewer schema --path <PATH>

Options:
      --path <PATH>  
  -h, --help         Print help
</code>
</pre>

### parquet 미리보기
<pre>
<code>
Usage: ddlviewer head [OPTIONS] --path <PATH>

Options:
      --path <PATH>                  
      --format <FORMAT>              [default: parquet]
      --count <COUNT>                [default: 10]
      --column-names <COLUMN_NAMES>  [default: *]
      --save-path <SAVE_PATH>        
      --max-cols <MAX_COLS>          [default: 100]
      --str-len <STR_LEN>            [default: 100]
  -h, --help                         Print help
</code>
</pre>

### parquet 질의하기
<pre>
<code>
Usage: ddlviewer head [OPTIONS] --path <PATH>

Options:
      --path <PATH>                  
      --format <FORMAT>              [default: parquet]
      --count <COUNT>                [default: 10]
      --column-names <COLUMN_NAMES>  [default: *]
      --save-path <SAVE_PATH>        
      --max-cols <MAX_COLS>          [default: 100]
      --str-len <STR_LEN>            [default: 100]
  -h, --help                         Print help
</code>
</pre>