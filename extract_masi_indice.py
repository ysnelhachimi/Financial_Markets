import os
import time
import bs4
from bs4 import BeautifulSoup
import pandas as pd

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from sqlalchemy import (
    create_engine
)

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_loading import dbsession, engine_lite

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_table import MasiIndices

from pyramido.pyrkanyon.pyrkanyon.models.helpers.p_factory import format, to_date


def get_days_imports_base(d, d2):
    df = extract_indice('MASI', d, d2)
    dres = pd.DataFrame(df)
    ldates = []
    for el in dres['seance']:
        ldates.append(el)
    return ldates



dico_indice = {
    'AGRO' : 'AGROALIMENTAIRE / PRODUCTION ',
    'ASSUR' : 'ASSURANCES',
    'BANK' : 'BANQUES',
    'B&MC' : 'BATIMENT & MATERIAUX DE CONSTRUCTION',
    'BOISS' : 'BOISSONS',
    'ESGI' : 'Casablanca ESG 10',
    'CHIM' : 'CHIMIE',
    'DISTR' : 'DISTRIBUTEURS',
    'ELEC' : 'ELECTRICITE',
    'EEE' : 'EQUIPEMENTS ELECTRONIQUES & ELECTRIQUES ',
    'ESGIO' : 'ESGI OUVERTURE',
    'PHARM' : 'INDUSTRIE PHARMACEUTIQUE',
    'I&BEI' : 'INGENIERIES & BIENS D’EQUIPEMENT INDUSTRIELS',
    'L&H' : 'LOISIRS ET HOTELS',
    'MADX' : 'MADEX',
    'MADXR' : 'MADEX RENTABILITE BRUT',
    'MADRN' : 'MADEX RENTABILITE NET',
    'MASI' : 'MASI',
    'MASIE' : 'MASI (EUR)',
    'MASID' : 'MASI (USD)',
    'MASIO' : 'MASI OUVERTURE',
    'MASIR' : 'MASI RENTABILITE BRUT',
    'MASRN' : 'MASI RENTABILITE NET',
    'L&SI' : 'MATERIELS,LOGICIELS & SERVICES INFORMATIQUES',
    'MINES' : 'MINES',
    'MSI20' : 'Morocco Stock Index 20',
    'IMMOB' : 'PARTICIPATION ET PROMOTION IMMOBILIERES',
    'P&G' : 'PETROLE & GAZ',
    'SAC' : 'SERVICES AUX COLLECTIVITES',
    'SDT' : 'SERVICES DE TRANSPORT',
    'SF&AF' : 'SOCIETE DE FINANCEMENT & AUTRES ACTIVITES FINANCIERES',
    'SPI' : 'SOCIETES DE PLACEMENT IMMOBILIER',
    'SP&H' : 'SOCIETES DE PORTEFEUILLES - HOLDINGS',
    'S&P' : 'SYLVICULTURE & PAPIER',
    'TCOM' : 'TELECOMMUNICATIONS',
    'TRANS' : 'TRANSPORT'
    }


INDICE_COLS = [
    'seance',
    'instrument',
    'variation'
]

URL_INDICE = "http://www.casablanca-bourse.com/bourseweb/indice-historique.aspx?Cat=22&IdLink=299"

dbs = dbsession()


def extract_table_indice(soup: bs4.BeautifulSoup, cols: list) -> list:
    table_rows = soup.find_all('tr')
    rv = []

    for row in table_rows[1:]:
        values = [x.text for x in row.find_all('span')]
        dico_row = dict(zip(cols, values))
        rv.append(dico_row)

    return rv

def sanitize_float(sfloat: str) -> float:
    rv = float(sfloat.replace(',', '.'))
    return rv

def sanitize_table_indice(records: list) -> list:
    rv = []

    for el in records:
        el['seance'] = to_date(el['seance'])
        #el['name'] = el['name'].strip().upper()
        el['instrument'] = sanitize_float(el['instrument'])
        el['variation'] = sanitize_float(el['variation'])
        rv.append(el)

    return rv

def extract_indice(indice, d_i, d_f):
    chromeOptions = webdriver.ChromeOptions()
    prefs = {
            'download.default_directory' : '/Users/ysn/Projects/kanyon/data/masi_files',
            'download.prompt_for_download': False,
            'safebrowsing.enabled': True,
            'directory_upgrade': True
            }
    chromeOptions.add_experimental_option('prefs', prefs)
    chromeOptions.add_argument("--headless")

    driver = webdriver.Chrome(ChromeDriverManager().install(), options= chromeOptions)

    #driver = webdriver.Chrome(path, options= chromeOptions)
    driver.get(URL_INDICE)

    elem = driver.find_element_by_xpath('//*[@id="IndiceHistorique1_RBSearchDate"]')
    date_ini = driver.find_element_by_xpath('//*[@id="IndiceHistorique1_DateTimeControl1_TBCalendar"]')
    date_fini = driver.find_element_by_xpath('//*[@id="IndiceHistorique1_DateTimeControl2_TBCalendar"]')
    downlo = driver.find_element_by_xpath('//*[@id="IndiceHistorique1_LinkButton1"]')
    listo = driver.find_element_by_xpath('//*[@id="IndiceHistorique1_DDLIndice"]')

    driver.execute_script("arguments[0].click()", elem)
    driver.execute_script(f"arguments[0].value = '{indice}'", listo)
    driver.execute_script(f"arguments[0].value='{d_i}'", date_ini)
    driver.execute_script(f"arguments[0].value='{d_f}'", date_fini)
    driver.execute_script("arguments[0].click()", downlo)
    time.sleep(3)
    driver.close()

    os.chdir('/Users/ysn/Projects/kanyon/data/masi_files')
    xml_file = 'Telechargement-indice.aspx'
    xml_data = open(xml_file,'r+', encoding='utf-8')

    data = xml_data.read()
    data = data.encode('ascii','ignore').decode('utf-8','ignore')
    soup = bs4.BeautifulSoup(data, 'html.parser')

    rows = extract_table_indice(soup, INDICE_COLS)
    rv = sanitize_table_indice(rows)
    return rv

def extract_implement_indice(d_i, d_f):
    '''
        functions main usage is to import 3 categories of data masi from "Bourse de Casablanca"
            from kanyon.data.db_imports.import_masi.extract_masi_indice import extract_implement_indice
            from kanyon.data.db_imports.import_masi.extract_masi_compo import extract_implement_compo
            from kanyon.data.db_imports.import_masi.extract_masi_volume import extract_implement_volume
            import pandas as pd

        sample:
        NB: format date is dd/mm/Y
            d_i = '12/06/2021'
            d_f = '08/07/2021'

            extract_implement_indice(d_i, d_f)
            extract_implement_volume(d_i, d_f)
            extract_implement_compo(d_i, d_f)

        params:
        d_i : date initiale
        d_f : date finale
        objectif: indice to db
    '''

    # --- base --- #
    quer_masi_ind = dbs.query(MasiIndices)
    dquer_masi_ind = pd.read_sql(quer_masi_ind.statement, quer_masi_ind.session.bind)
    dquer_masi_ind_rt = dquer_masi_ind.drop(columns='id')
    dquer_masi_ind_rts = dquer_masi_ind_rt.sort_values(['seance'])
    dquer_masi_ind_rtsd = dquer_masi_ind_rts.drop_duplicates(['seance','name','instrument','variation']).sort_values(['seance'])

    first_date_indb = dquer_masi_ind_rtsd.seance[0]
    last_date_indb = dquer_masi_ind_rtsd.seance[len(dquer_masi_ind_rtsd.seance)-1]

    list_dates_masi_ind = list(dquer_masi_ind_rtsd.seance)


    if (to_date(d_i) in list_dates_masi_ind) and (to_date(d_f) in list_dates_masi_ind):
        print(f'indices_existants du {d_i} of {d_f}',
            f'premiere date:{first_date_indb}',
            f'dernier date:{last_date_indb}'
            )

    elif (to_date(d_i) in list_dates_masi_ind) and (to_date(d_f) not in list_dates_masi_ind) and (to_date(d_f) > last_date_indb):

            list_dates = get_days_imports_base(d_i, d_f)
            list_dates_res = []
            for el_date in list_dates:
                if el_date > last_date_indb:
                    list_dates_res.append(el_date)

            first_list_dates_res = list_dates_res[0]
            last_list_dates_res = list_dates_res[len(list_dates_res)-1]

            print(
                f'## - données à importer du : {first_list_dates_res} au {last_list_dates_res}',
                f'## - premiere date in db :{first_date_indb}',
                f'## - dernier date in db :{last_date_indb}'
                )
            for el in dico_indice.keys():
                print(el)
                try:
                    res = extract_indice(el, first_list_dates_res, last_list_dates_res)
                    dres = pd.DataFrame(res)
                    dres.insert(1, 'name', el)

                    dres.to_sql('masi_indices', con = engine_lite, if_exists='append', index = False)

                    print(
                        f"Done import indice : {el} -- du {first_list_dates_res} au {last_list_dates_res}"
                        )
                    try:
                        os.remove('Telechargement-indice.aspx')
                    except:
                        print("pas de fichier en cours")

                except:
                    print(f'probleme dimportation masi_indices {el}')

    else:
        for el in dico_indice.keys():
            try:
                res = extract_indice(el, d_i, d_f)
                dres = pd.DataFrame(res)
                dres.insert(1, 'name', el)

                dres.to_sql('masi_indices', con = engine_lite, if_exists='append', index = False)

                print(
                    f"Done import indice : {el} -- du {d_i} au {d_f}",
                    f'premiere date:{first_date_indb}',
                    f'dernier date:{last_date_indb}'
                    )
                try:
                    os.remove('Telechargement-indice.aspx')
                except:
                    print("pas de fichier en cours")

            except:
                print(f'probleme dimportation {el}')
