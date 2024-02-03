if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):    
    
    print(f"Preprocessing: rows with zero passengers: {data['passenger_count'].isin([0]).sum()}")
    print(f"Preprocessing: rows with zero passengers: {data['trip_distance'].isin([0]).sum()}")

    # filter only valid trip  
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    # rename columns
    data.rename(columns={
        "VendorID": "vendor_id",
        "RatecodeID": "ratecode_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id"
    }, inplace=True)

    print(f"Vendor ID distinct values { data['vendor_id'].unique() }")
    
    return data


@test
def test_output(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, "There are rides with zero passengers"
    assert output['trip_distance'].isin([0]).sum() == 0, "There are rides with zero distance"
    assert "vendor_id" in output, "Column vendor_id not exists in output"
