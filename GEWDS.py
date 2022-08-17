import pandas as pd
import os
from functools import cache
from time import sleep

@cache
def region_dir():
    df = pd.read_json('https://eleksyonconfig2.gmanetwork.com/gno/microsites/eleksyon2019/results/ref/NO/geolocation_REGION.json')
    df = df.rename(columns={'REGION':'region_name'})
    df = df.drop(['registered_voters'], axis=1)
    df['region_code'] = [region.replace(" ","_") for region in df['region_name']]
    region_dir = dict(zip(df['region_name'],df['region_code']))
    return region_dir

@cache
def province_dir(region):
    region_code = region_dir().get(region)
    ref = region_code[:-3:-1]
    df = pd.read_json(f'https://eleksyonconfig2.gmanetwork.com/gno/microsites/eleksyon2019/results/ref/{ref}/geolocation_{region_code}.json')
    df = df.rename(columns={'PROVINCE':'province_name'})
    df = df.drop(['registered_voters'], axis=1)
    df['province_code'] = [province.replace(" ","_").replace("(","_").replace(".","_").replace(")","_") for province in df['province_name']]
    province_dir = dict(zip(df['province_name'],df['province_code']))
    return province_dir

@cache
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

@cache
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
    os.makedirs(path)

def main():
    BASE_PATH = "GMAEleksyon2019Data"
    BASE_URL = 'https://eleksyondata2019.gmanews.tv/all_lvgs_results'
    DOWNLOAD_DELAY = 1
    
    for region in region_dir().keys():
        region_code = region_dir().get(region)
        reg_path = f"{BASE_PATH}/{region}"
        if os.path.exists(reg_path) == False:
            create_directory(reg_path)
        if os.path.exists(f'{reg_path}/{region}.json') == False:
            reg_result = pd.read_json(f'{BASE_URL}/{region_code}.json')
            print(f'Getting results from {region}...')
            sleep(DOWNLOAD_DELAY)
            reg_result.to_json(f'{reg_path}/{region}.json', orient='records')
        else:
            print(f"{reg_path}/{region}.json exists. ")
            
            
        for province in province_dir(region).keys():
            province_code = province_dir(region).get(province)
            prov_path = f"{reg_path}/{province}"
            if os.path.exists(prov_path) == False:
                create_directory(prov_path)
            if os.path.exists(f'{prov_path}/{province}.json') == False:
                prov_result = pd.read_json(f'{BASE_URL}/{region_code}_{province_code}.json')
                print(f'Getting results from {province}, {region}...')
                sleep(DOWNLOAD_DELAY)
                prov_result.to_json(f'{prov_path}/{province}.json', orient='records')
            else:
                print(f"{prov_path}/{province}.json exists. ")
                
                
            for citymun in citymun_dir(region,province).keys():
                citymun_code = citymun_dir(region,province).get(citymun)
                citymun_path = f"{prov_path}/{citymun}"
                if os.path.exists(citymun_path) == False:
                    create_directory(citymun_path)        
                if os.path.exists(f'{citymun_path}/{citymun}.json') == False:
                    citymun_result = pd.read_json(f'{BASE_URL}/{region_code}_{province_code}_{citymun_code}.json')
                    print(f'Getting results from {citymun}, {province}, {region}...')
                    sleep(DOWNLOAD_DELAY)
                    citymun_result.to_json(f'{citymun_path}/{citymun}.json', orient='records')
                else:
                    print(f"{citymun_path}/{citymun}.json exists. ")
                    
                for barangay in barangay_dir(region,province,citymun).keys():
                    brgy_code = barangay_dir(region,province,citymun).get(barangay)
                    brgy_path = f"{citymun_path}/{barangay}"
                    if os.path.exists(brgy_path) == False:
                        create_directory(brgy_path)        
                    if os.path.exists(f'{brgy_path}/{barangay}.json') == False:
                        df = pd.read_json(f'{BASE_URL}/{region_code}_{province_code}_{citymun_code}_{brgy_code}.json')
                        print(f'Getting results from {barangay}, {citymun}, {province}, {region}...')
                        sleep(DOWNLOAD_DELAY)
                        df.to_json(f'{brgy_path}/{barangay}.json', orient='records')
                    else:
                        print(f"{brgy_path}/{barangay}.json exists. ")
                        
    print("Download Finished!")

if __name__ == "__main__":
    main()
