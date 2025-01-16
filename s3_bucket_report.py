import boto3
import csv

def get_bucket_info():
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()['Buckets']
    report = []

    for bucket in buckets:
        bucket_name = bucket['Name']
        
        # Get lifecycle rules
        try:
            lifecycle = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            lifecycle_rules = lifecycle.get('Rules', [])
        except s3.exceptions.ClientError:
            lifecycle_rules = "No lifecycle rules"

        # Get bucket size and storage type
        size_gb = 0
        storage_classes = {}
        objects = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                size_gb += obj['Size'] / (1024 ** 3)  # Convert to GB
                storage_class = obj.get('StorageClass', 'STANDARD')
                storage_classes[storage_class] = storage_classes.get(storage_class, 0) + obj['Size']

        # Format report entry
        report.append({
            'Bucket Name': bucket_name,
            'Size (GB)': round(size_gb, 2),
            'Storage Classes': storage_classes,
            'Lifecycle Rules': lifecycle_rules
        })

    return report

if __name__ == "__main__":
    report = get_bucket_info()
    for bucket in report:
        print(bucket)

with open('s3_bucket_report.csv', 'w', newline='') as csvfile:
    fieldnames = ['Bucket Name', 'Size (GB)', 'Storage Classes', 'Lifecycle Rules']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(report)

