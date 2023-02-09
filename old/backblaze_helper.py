def upload_file(b2, bucket_name, filename, base_url, file):
    file.seek(0)

    bucket = b2.get_bucket_by_name(bucket_name)
    is_duplicate = False

    file_info = _get_file_in_bucket(bucket, filename)

    if not file_info:
        file_info = bucket.upload_bytes(file.getvalue(), file_name=filename)
    else:
        is_duplicate = True

    url = '{}/{}'.format(base_url, file_info.file_name)

    return url, is_duplicate


def _get_file_in_bucket(bucket, filename):
    try:
        file_info = bucket.get_file_info_by_name(filename)
        return file_info
    except Exception:
        return None


def get_file_in_bucket(b2, bucket_name, filename):
    try:
        bucket = b2.get_bucket_by_name(bucket_name)
        file_info = bucket.get_file_info_by_name(filename)
        return file_info
    except Exception:
        return None
