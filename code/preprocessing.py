import gc
import os
import functools
import tqdm
import shutil
import requests
import pandas as pd
import numpy as np

from pyunpack import Archive
from bs4 import BeautifulSoup
from pathlib import Path
from multiprocessing import Pool
from dbfread import DBF
from typing import Union


def load_and_unpack(url:str,
                    file_name:str, 
                    load_path:str, 
                    save_path:str, 
                    overwrite:bool=True) -> None:
    '''
    Loads and unpacks archive from given url
    
    Parameters
    ----------
    url: str
            Url to load file from
    file_name: str
            File name to save file
    load_path: str
            Folder to save rar or zip archives from CBR site
    save_path: str
            Folder to save unpacked .dbf files
    override_data: bool
            Whether to overwrite data in folder if it already exists
    '''

    out = requests.get(url, stream=True)
    archive_path = Path(load_path) / file_name
  
    # сохраним архив
    with open(archive_path, 'wb') as f:
        f.write(out.content)

    # распакуем архив с .dbf-файлами в папку
    unzip_path = Path(save_path) / file_name
    # создадим папку для сохранения распакованного архива
    if Path.exists(unzip_path) and not overwrite:
        print('The folder already exists, no data will be added to existing files')
    else:
        Path.mkdir(unzip_path)
        # распакуем
        Archive(archive_path).extractall(unzip_path)


def load_bank_statements(form_number:int, 
                         filepath: str,
                         overwrite:bool=True) -> None:
    '''
    Loads archives with bank statements from CBR website and unpacks them into given folder
    
    Parameters
    ----------
    form_number: int, 101 or 102
            Number of CBR form of financial statements (form 101, form 102)
    filepath: str
            Directory (folder) to save .zip and .rar archives  downloaded from CBR site
            as well as unpacked .dbf files from archives
    overwrite: bool
            Whether to overwrite directories with archives and unzipped data if they already
            exist (option overwrite=False is currently not available)
    '''

    print('Downloading and unpacking files from www.cbr.ru, please be patient...')
    
    url = 'https://cbr.ru/banking_sector/otchetnost-kreditnykh-organizaciy/'
    
    # create directories to save data
    load_path = Path(filepath) / (str(form_number) +'_zipped')
    save_path = Path(filepath) / str(form_number)
    # delete directories if they already exist
    if overwrite:
        if load_path.is_dir():
            shutil.rmtree(load_path, ignore_errors=True)
        if save_path.is_dir():
            shutil.rmtree(save_path, ignore_errors=True)
            
    # create new empty folders for data instead of the old folders
    Path(load_path).mkdir(parents=True, exist_ok=True)
    Path(save_path).mkdir(parents=True, exist_ok=True)

    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, 'html')

    # выберем все ссылки для нужной формы отчётности
    all_refs = [x['href'] for x in soup.find_all('a', href=True)]
    refs = ['https://cbr.ru/' + x for x in all_refs \
            if 'forms/' + str(form_number) in x]
    
    # сгенерируем кортежи аргументов для параллельной функции
    args = ((x, 
             (
             (x.split('/')[-1]).split('.')[0]
             ).split('-')[1], 
             load_path, 
             save_path,
             True) for x in refs)
    
    # параллельные действия
    # if __name__=='__main__':
    with Pool() as pool:
        pool.starmap(load_and_unpack, 
                     # to show progress bar
                     tqdm.tqdm(args, total=len(refs)))
            
    print('Congratulations! Finished.')
            
    # на всякий случай сохранил альтернативный вариант с циклом
    # он работает в 4 раза медленнее, чем распараллеленный вариант
    
    #for ref in refs:
    #    load_and_unpack(ref,
    #                    file_name=(ref.split('/')[-1]).split('.')[0],
    #                    load_path=load_path,
    #                    save_path=save_path)


def dbf2df(filepath: str, encoding:str) -> pd.DataFrame:
    """
    Reads .dbf from given filepath into dataframe and returns df
    """
    dbf = DBF(filepath, encoding=encoding)
    df = pd.DataFrame(iter(dbf)) 
    # if there is no 'DT' column with date
    if 'DT' not in df.columns:
        # get date from folder name
        date = str(filepath).split('\\')[-2]
        df['DT'] = pd.to_datetime(date)
    return df
    

def get_filepaths(filepath:str):
    """Returns full paths to files in the given folder with form 101 or 102"""
    folders = [filelist[0] for filelist in os.walk(filepath)][1:]
    files = []
    for folder in folders:
        for file in os.listdir(folder):
            files.append(Path(folder)/file)
    return files


def get_bank_names(filepath:str, 
                  form_number:int, 
                  encoding:str='cp866') -> pd.DataFrame:
    """
    Collects all bank names and register numbers from given form 
    (from files with 'N1' for form 101 and 'NP1' for form 102). Returns 
    Pandas DataFrame with 2 columns: register number (REGN) and name 
    of the bank.
    
    Parameters
    ----------
    path: str
            Path to folder with folders with downloaded and unzipped files
    encoding:str, default 'cp866'
            Encoding to open .dbf files from CBR. Recommended value is 'cp866'
    form_number: int, 101 or 102
            Whether to collect bank names from files with 101 or 102 form
    """
    # collect all file paths from folders in given path
    files = get_filepaths(filepath)
            
    # different files with names for different forms
    search = {101:'N1', 102:'NP1'}
    # find all files with matching pattern in file names
    names = list(filter(lambda x: \
                        search[form_number] in str(x), files))
    
    # read all these files and merge them into one file
    df = functools.reduce(lambda a,b: \
                          pd.concat((a.reset_index(drop=True), 
                                     b.reset_index(drop=True))),
                          [dbf2df(x, encoding=encoding) for x in names])
    
    # integer codes ('REGN') are unique for all banks, but 
    # the same bank names are sometimes written in different ways
    df.drop_duplicates(subset='REGN', inplace=True)
    return df[['REGN', 'NAME_B']].reset_index(drop=True)


def read_form(filepath:str, 
              form_number:int, 
              which_files:str=None, 
              remove_unknown_accs:bool=True,
              to_int:bool=True, 
              encoding:str='cp866') -> pd.DataFrame:
    '''
    Reads and merges all .dbf files for given form and filepath. Returns merded
    dataframe.

    Parameters
    ----------
    filepath: str
            Directory (folder) where are stored .dbf files for form 101 or 102
    form_number: int, 101 or 102 
            Number of CBR form of financial statements (form 101, form 102 etc)
    which_files: str, default None
            Search pattern to look for in file names. For example, by default the 
            function opens and merges all files with 'B1' in filename for 
            form  101 and all files with '_P1' in filenames for 102 form. You can set your 
            own search pattern, but you should be sure, that all files with that pattern
            have the same column names and column order, otherwise function will return
            garbage.
    remove_unknown_accs: bool, default True
            Whether to remove unknown accounts from columns with account number. 
            There are some accounts in form 101 whose meaning I could not find 
            in the Central Bank documents. These accounts are 'ITGAP', '304.1', '408.1', 
            '408.2', '474.1', '1XXXX', '2XXXX', '3XXXX', '4XXXX', '5XXXX', '6XXXX',
            '7XXXX', '8XXXX', '9XXXX'. Removing them allows us to convert column with
            account number from string to integer. This conversion boosts performance
            in dataframe processing and memory management.
    to_int: bool, default True
            Whether to convert column with account numbers to int.
    enconding: str, default 'cp866'
            Encoding to open .dbf files. 'cp866' works well with form 101 and 102
    '''
    
    print('Reading .dbf files from your PC, please wait...')

    files = get_filepaths(filepath)
    # different files with names for different forms
    search = {101:'B1', 102:'_P1'}
    # columns with account numbers
    acc_cols = {101:'NUM_SC', 102:'CODE'}
    # accounts to remove if remove_unknown_accs=True
    remove_accounts = ['ITGAP', '304.1', '408.1', '408.2', '474.1'] + \
                      [str(x)+'XXXX' for x in range(1, 9, 1)]
    # find all files with matching pattern in file names
    if which_files:
        search_str = which_files
    else:
        search_str = search[form_number]

    names = list(filter(lambda x: search_str in str(x), files))
    
    args = ((name, encoding) for name in names)
    
    with Pool() as pool:
        dfs = list(pool.starmap(dbf2df, 
                     # to show progress bar
                     tqdm.tqdm(args, total=len(names))))

    print('Opened files. Merging them...')

    df = pd.concat(dfs)
    # delete large list of files from memory
    del dfs
    gc.collect()

    # make date column to datetime index
    df.index = pd.to_datetime(df['DT'])
    df.sort_index(inplace=True)
    df.drop(columns='DT', inplace=True)
    # remove unknown accounts and convert account numbers to int
    if all([remove_unknown_accs, form_number==101]):
        df = df[~df['NUM_SC'].isin(remove_accounts)]
    # convert account numbers column to integer
    if to_int:
        if not remove_unknown_accs and form_number==101:
            raise TypeError(
                """
                You have not removed some very specific accounts
                (remove_unknown_accs=False). This accounts (for 
                instance, 3XXXX) can not be converted to integer.
                """
                )
        df[acc_cols[form_number]] = df[acc_cols[form_number]].astype('int32')

    print('Done.')
    return df


def group(data:pd.DataFrame, 
          aggschema:Union[dict, pd.DataFrame], 
          form:int, 
          acc_col:str=None,
          agg_col:str=None,
          reg_col:str='REGN',
          date_col:str='DT', 
          aggfunc:str='sum') -> pd.DataFrame:
    """
    Returns dataframe with accounts values grouped and sumed by 
    unique bank register number, date and aggschema supplied by user
    
    Parameters:
    -----------
    data:pd.DataFrame
        Dataframe to group. Contains at least 4 columns with:
            - bank register numbers
            - dates
            - old account codes (integer datatype)
            - values for old account codes
    aggschema:dict or Pandas DataFrame
        Dictionary of DataFrame wich maps accounts in the data to 
        the grouped accounts for analytical purposes. 
        Example for dict:
            aggschema = {'Retail credits': [45502, 45508, 45509]},
            where 'Retail credits' is the new account name, and 
            45502, 45508, 45509 are old account numbers to be grouped
            into one 'Retail credit' account for each bank in the table.
        Example for DataFrame:
                   new_code    old_code
            0  'Retail credit'  45502
            1  'Retail credit'  45508
            2  'Retail credit'  45509
        The DataFrame should contain new account in the first column and 
        old accounts in the second. The names of the columns are not important.
    form:int, 101 or 102
        CBR form number.
    acc_col:str, default None
        Use specific account column name instead of 'NUM_SC' for form 101
        or 'CODE' for form 102.
    agg_col:str, default None
        Use specific aggregation column name (with numbers to aggregate)
        instead of 'SIM_ITOGO' for form 101 or 'IITG' for form 102.
    date_col:str, defaul 'DT'
        Date column name in dataframe with data. Default 'DT' (both 
        in form 101 and 102)
    aggfunc:str, default 'sum'
        Function to aggregate existing accounts into new accounts.
    """
    account_cols={101:'NUM_SC', 102:'CODE'}
    aggcols = {101:'IITG', 102:'SIM_ITOGO'}
    # if custom account or aggregation columns are submitted
    if acc_col:
        account_cols[form] = acc_col
    elif agg_col:
        aggcols[form] = agg_col

    if isinstance(aggschema, dict):
        newdict = {y:x[0] for x in aggschema.items() for y in x[1]}
        aggschema = pd.DataFrame({'new_code': newdict.values(),
                                  account_cols[form]: newdict.keys()})
    elif isinstance(aggschema, pd.DataFrame):
        aggschema.columns=['new_code', account_cols[form]]

    print('Grouping and aggregating data. Please be patient...')

    df = pd.merge(left=data.reset_index(), 
                  right=aggschema, 
                  how='left', 
                  left_on=account_cols[form], 
                  right_on=account_cols[form])
    
    df.set_index(date_col, inplace=True)
    df.dropna(subset=[account_cols[form]], inplace=True)
    df = df.groupby(by=[reg_col, 
                        df.index, 
                        'new_code']).agg({aggcols[form]:aggfunc})
    
    df.reset_index(inplace=True)
    df.set_index(date_col, inplace=True)

    print('Finished.')
    
    return df
