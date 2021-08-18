import boto3

def handler(event, context):
    """
    lambda function that starts a job flow in EMR
    """
    client = boto3.client('emr', region_name='us-east-2')

    cluster_id = client.run_job_flow(
        Name='EMR-Tancredo-IGTI-delta',
        ServiceRole='EMR_Default_Role',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        LogUri='s3://datalake-igti-tf-tancredo-2021/emr-logs',
        ReleaseLabel='emr-6.3.0',
        isinstances={
            'InstanceGroups': [
                {
                    'Name': 'Master nodes',
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge'
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Worker nodes',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge'
                    'InstanceCount': 1,
                },
            ],
            'Ec2KeyName': 'tancredo-igti-teste',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-289a0f43'
        },

        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hive'},
            {'Name': 'Pig'},
            {'Name': 'Hue'},
            {'Name': 'JupyterHub'},
            {'Name': 'JupyterEnterpriseGateway'},
            {'Name': 'Livy'},
        ],

        Configurations=[{
            "Classification": "spark-env",
            "properties": {},
            "Configurations": [{
                "Classification": "export",
                "properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3",
                    "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                }
            }]
        },
            {
                "Classification": "spark-hive-site",
                "properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                }
            },
            {
                "Classification": "spark-defaults",
                "properties": {
                    "spark.submit.deployMode": "cluster",
                    "spark.speculation": "false",
                    "spark.sql.adaptive.enable": "true",
                    "spark.serializer": "org.apache.spark.serializer.kryoSerializer"
                }
            },
            {
                "Classification": "spark",
                "properties": {
                    "maximizeResourceAllocation": "true"
                }
            }       
        ],

        StepConcurrencylevel=1,

        Steps=[{
            'Name': 'Delta Insert do ENEM',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                        '--packages', 'io.delta:delta-core_2.12:1.0.0',
                        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSession',
                        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                        '--master', 'yarn',
                        '--deploy-mode', 'cluster',
                        's3://datalake-igti-tf-tancredo-2021/emr-code/pyspark/01_delta_spark_insert.py'
                        ]
                }
            },
            {
            'Name': 'Simulação e UPSERT do ENEM',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                        '--packages', 'io.delta:delta-core_2.12:1.0.0',
                        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSession',
                        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                        '--master', 'yarn',
                        '--deploy-mode', 'cluster',
                        's3://datalake-igti-tf-tancredo-2021/emr-code/pyspark/02_delta_spark_upsert.py'
                        ]
                }
            }],
        )

    return {
        'statusCode': 200,
        'body': f"Started job flow {cluster_id['JobFlowId']}"
    }