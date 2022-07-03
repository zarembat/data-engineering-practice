import boto3
import gzip


s3 = boto3.client('s3')


def get_s3_obj(bucket: str, key: str) -> dict:
    return s3.get_object(
        Bucket=bucket,
        Key=key
        )


def main():
    
    bucket = 'commoncrawl'

    # Get the 1st object
    wet_paths_gz = get_s3_obj(bucket, 'crawl-data/CC-MAIN-2022-05/wet.paths.gz')

    # Ungzip file
    with gzip.GzipFile(fileobj=wet_paths_gz['Body']) as main_file:
        # Read the first line
        obj = next(main_file).decode('UTF-8').strip()
        # Get the other file
        s3_obj = get_s3_obj(bucket, obj)
        # Ungzip file
        with gzip.GzipFile(fileobj=s3_obj['Body']) as target_file:
            for line in target_file:
                # Convert to a string and remove the newline character at the end
                line_str = line.decode('UTF-8').strip()
                # Print each line
                print(line_str)


if __name__ == '__main__':
    main()
