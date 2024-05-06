def insert_data_log_path():
    return """
    INSERT INTO log1(
        file_name,
        s3_object
    )
    VALUES (:file_name, :s3_object)

    """