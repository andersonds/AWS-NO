import boto3
import csv

def get_bucket_info():
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()['Buckets']
    report = []

    for bucket in buckets:
        bucket_name = bucket['Name']
        
        # Pegar regras de LifeCycle do S3
        try:
            lifecycle = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
            lifecycle_rules = lifecycle.get('Rules', [])
        except s3.exceptions.ClientError:
            lifecycle_rules = "No lifecycle rules"

        # Pega tamanho e tipo de Bucket
        size_mb = 0
        storage_classes = {}
        objects = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                size_mb += obj['Size'] / (1024 ** 2)  # Converte para MB
                storage_class = obj.get('StorageClass', 'STANDARD')
                storage_classes[storage_class] = storage_classes.get(storage_class, 0) + obj['Size']

        # Formata relatório
        report.append({
            'Nome do Bucket ': bucket_name,
            'Tamanho (GB)': round(size_mb, 2),
            'Classe de Storage': storage_classes,
            'Regra de Lifecycle': lifecycle_rules
        })

    return report

if __name__ == "__main__":
    report = get_bucket_info()
    for bucket in report:
        print(bucket)

# Gerar o arquivo CSV
with open('s3_bucket_report.csv', 'w', newline='') as csvfile:
    fieldnames = ['Nome do Bucket', 'Tamanho (MB)', 'Classe de Storage', 'Regra de Lifecycle']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(report)

