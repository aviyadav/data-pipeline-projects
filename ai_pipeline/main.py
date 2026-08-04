from pipeline.settings import settings
from pipeline.ingest import ingest
from pipeline.transform import get_purchases
from pipeline.features import build_features
from pipeline.export import export

def main():
    settings.setup_dirs()
    ingest()
    purchases = get_purchases()
    features = build_features(purchases)
    export(features)

if __name__ == "__main__":
    main()
