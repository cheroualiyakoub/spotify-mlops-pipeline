from dagster import repository, job, asset, AssetIn



@asset(ins={"my_asset": AssetIn()})
def my_asset_5(my_asset):
    print(f"Hello world from asset! Received 4#: {my_asset}")
    return f"Asset executed successfully with input 4#: {my_asset}"



@asset(ins={"my_asset": AssetIn()})
def my_asset_4(my_asset):
    print(f"Hello world from asset! Received 4#: {my_asset}")
    return f"Asset executed successfully with input 4#: {my_asset}"


@asset(ins={"my_asset": AssetIn()})
def my_asset_3(my_asset):
    print(f"Hello world from asset! Received #: {my_asset}")
    return f"Asset executed successfully with input #: {my_asset}"

@asset
def my_asset_2():
    print("Hello world from asset 2!")
    return "Asset 2 executed successfully"

@asset(ins={"my_asset_2": AssetIn()})
def my_asset(my_asset_2):
    print(f"Hello world from asset! Received: {my_asset_2}")
    return f"Asset executed successfully with input: {my_asset_2}"



@job
def my_pipeline():
    print("Hello world from pipeline!")
    pass

@repository
def my_repo():
    return [my_pipeline, my_asset, my_asset_2, my_asset_3, my_asset_4, my_asset_5]