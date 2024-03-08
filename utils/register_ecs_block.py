from prefect_aws import ECSTask
import os

# Ensure infrastructure variable passed are still valid!
my_task = ECSTask(  # Need refactor to variable.
    vpc_id="vpc-018a52b77cf0fc715",
    cluster="arn:aws:ecs:ap-northeast-1:211125707335:cluster/prefect-agent-stage",
    image="211125707335.dkr.ecr.ap-northeast-1.amazonaws.com/prefect-flows:latest",
    execution_role_arn="arn:aws:iam::211125707335:role/prefect-agent-execution-role-stage",
    task_role_arn="arn:aws:iam::211125707335:role/prefect-agent-task-role-stage",
    task_start_timeout_seconds=400,
    env={
        "PREFECT_LOGGING_LEVEL": "DEBUG",
        "APP_ENV": "stage",
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
    configure_cloudwatch_logs=True,
)

my_task.save("prefect-flows-stage", overwrite=True)
