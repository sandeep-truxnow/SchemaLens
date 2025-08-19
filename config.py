"""Configuration settings"""

# Environment configurations
ENVIRONMENTS = {
    "QA": {
        "target": "i-0b51f34b778499f27",
        "host": "qa-db-cluster.cluster-cv4ii4ai2san.us-east-2.rds.amazonaws.com",
        "region": "us-east-2",
        "local_port": "3307"
    },
    "UAT": {
        "target": "i-0961407722d75ecf7",
        "host": "uat-ecs-prod-cluster-2024-09-26-9-55-am-mst-cluster.cluster-cpjdjlxf8yyr.us-west-2.rds.amazonaws.com",
        "region": "us-west-2",
        "local_port": "3308"
    },
    "PROD": {
        "target": "i-06d45207a10bdca28",
        "host": "ecs-prod-db-cluster.cluster-ro-c8lvt6dvdsj6.us-east-2.rds.amazonaws.com",
        "region": "us-east-2",
        "local_port": "3309"
    }
}

# Fixed connection parameters
CONNECTION_CONFIG = {
    "db_type": "MySQL",
    "host": "localhost",
    "port": "3307",
    "username": "autotrux",
    "password": "autotrux-pw"
}