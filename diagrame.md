flowchart TD
    subgraph Docker & Infra
        A1[Docker Compose]
        A2[PostgreSQL]
        A3[LakeFS]
        A4[MLflow]
        A5[Dagster]
        A6[FastAPI]
        A7[Streamlit]
    end

    subgraph Data Lake & Versioning
        B1[LakeFS Repo<br>Branches: raw, split, dev, ...]
        B2[LakeFS Storage<br>(local/ S3)]
    end

    subgraph Orchestration & Pipeline
        C1[Dagster Assets<br>data_ingestion.py]
        C2[Dagster Assets<br>data_selection.py]
        C3[Dagster Assets<br>model_training.py]
        C4[Dagster Assets<br>model_evaluation.py]
        C5[LakeFS IO Manager]
    end

    subgraph ML & Serving
        D1[MLflow Tracking]
        D2[FastAPI Model API]
        D3[Streamlit Frontend]
    end

    %% Connections
    A1 --> A2
    A1 --> A3
    A1 --> A4
    A1 --> A5
    A1 --> A6
    A1 --> A7

    A3 <--> B1
    B1 <--> B2

    A5 --> C1
    A5 --> C2
    A5 --> C3
    A5 --> C4
    C1 --> B1
    C2 --> B1
    C3 --> B1
    C4 --> B1
    C1 --> C5
    C2 --> C5
    C3 --> C5
    C4 --> C5

    C3 --> D1
    D1 --> D2
    D1 --> D3
    D2 --> A6
    D3 --> A7