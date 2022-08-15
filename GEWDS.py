import pandas as pd
import os

BASE_PATH = "GMAEleksyon2019Data"

def region_dir():
    df = pd.read_json('https://eleksyonconfig2.gmanetwork.com/gno/microsites/eleksyon2019/results/ref/NO/geolocation_REGION.json')
    df = df.rename(columns={'REGION':'region_name'})
    df = df.drop(['registered_voters'], axis=1)
    df['region_code'] = [region.replace(" ","_") for region in df['region_name']]
    region_dir = dict(zip(df['region_name'],df['region_code']))
    return region_dir
  
  def province_dir(region):
    region_code = region_dir().get(region)
    ref = region_code[:-3:-1]
    df = pd.read_json(f'https://eleksyonconfig2.gmanetwork.com/gno/microsites/eleksyon2019/results/ref/{ref}/geolocation_{region_code}.json')
    df = df.rename(columns={'PROVINCE':'province_name'})
    df = df.drop(['registered_voters'], axis=1)
    df['province_code'] = [province.replace(" ","_").replace("(","_").replace(".","_").replace(")","_") for province in df['province_name']]
    province_dir = dict(zip(df['province_name'],df['province_code']))
    return province_dir
  
  def citymun_dir(region,province):
    region_code = region_dir().get(region)
    province_code = province_dir(region).get(province)
    ref = province_code[:-3:-1]
    df = pd.read_json(f'https://eleksyonconfig2.gmanetwork.com/gno/microsites/eleksyon2019/results/ref/{ref}/geolocation_{region_code}_{province_code}.json')
    df = df.rename(columns={'MUNICIPALITY':'citymun_name'})
    df = df.drop(['registered_voters'], axis=1)
    df['citymun_code'] = [citymun.replace(" ","_").replace("(","_").replace(".","_").replace(")","_") for citymun in df['citymun_name']]
    citymun_dir = dict(zip(df['citymun_name'],df['citymun_code']))
    return citymun_dir
  
  def barangay_dir(region,province,citymun):
    region_code = region_dir().get(region)
    province_code = province_dir(region).get(province)
    citymun_code = citymun_dir(region,province).get(citymun)
    ref = citymun_code[:-3:-1]
    df = pd.read_json(f'https://eleksyonconfig2.gmanetwork.com/gno/microsites/eleksyon2019/results/ref/{ref}/geolocation_{region_code}_{province_code}_{citymun_code}.json')
    df = df.rename(columns={'BARANGAY':'brgy_name'})
    df = df.drop(['registered_voters'], axis=1)
    df['brgy_code'] = [barangay.replace(" ","_").replace("(","_").replace(".","_").replace(")","_") for barangay in df['brgy_name']]
    barangay_dir = dict(zip(df['brgy_name'],df['brgy_code']))
    return barangay_dir
  
  def create_directory(path):
    print(f"Creating path:{path}")
    os.makedirs(path)
    print("Path Created.")
    
  def download_data(region,province,citymun):
    region_code = region_dir().get(region)
    province_code = province_dir(region).get(province)
    citymun_code = citymun_dir(region,province).get(citymun)
    
    reg_path = f"{BASE_PATH}/{region}"
    if os.path.exists(reg_path) == False:
        create_directory(reg_path)
    reg_result = pd.read_json(f'https://eleksyondata2019.gmanews.tv/all_lvgs_results/{region_code}.json')
    print(f'Getting results from {region}...')
    reg_result.to_json(f'{reg_path}/{region}.json', orient='records')
    
    prov_path = f"{BASE_PATH}/{region}/{province}"
    if os.path.exists(prov_path) == False:
        create_directory(prov_path)
    prov_result = pd.read_json(f'https://eleksyondata2019.gmanews.tv/all_lvgs_results/{region_code}_{province_code}.json')
    print(f'Getting results from {province}, {region}...')
    prov_result.to_json(f'{prov_path}/{province}.json', orient='records')
    
    citymun_path = f"{BASE_PATH}/{region}/{province}/{citymun}"
    create_directory(citymun_path)
    citymun_result = pd.read_json(f'https://eleksyondata2019.gmanews.tv/all_lvgs_results/{region_code}_{province_code}_{citymun_code}.json')
    print(f'Getting results from {citymun}, {province}, {region}...')
    citymun_result.to_json(f'{citymun_path}/{citymun}.json', orient='records')
    
    for barangay in barangay_dir(region,province,citymun).keys():
        brgy_code = barangay_dir(region,province,citymun).get(barangay)
        brgy_path = f"{BASE_PATH}/{region}/{province}/{citymun}/{barangay}"
        create_directory(brgy_path)
        df = pd.read_json(f'https://eleksyondata2019.gmanews.tv/all_lvgs_results/{region_code}_{province_code}_{citymun_code}_{brgy_code}.json')
        print(f'Getting results from {barangay}, {citymun}, {province}, {region}...')
        df.to_json(f'{brgy_path}/{barangay}.json', orient='records')
    print('Download Completed.')
