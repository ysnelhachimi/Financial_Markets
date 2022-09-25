import bs4
import time
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

import pandas as pd

from sqlalchemy import (
    create_engine
)

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_loading import dbsession, engine_lite

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_table import MasiVolume

from pyramido.pyrkanyon.pyrkanyon.models.helpers.p_factory import format, to_date

from pyramido.pyrkanyon.pyrkanyon.models.data.db_imports.import_masi.extract_masi_indice import get_days_imports_base

PREFS = {
        'download.default_directory' : '/Users/ysn/Projects/kanyon/data/masi_files/',
        'download.prompt_for_download': False,
        'safebrowsing.enabled': True,
        'directory_upgrade': True
}

URL_VOLUMES = "http://www.casablanca-bourse.com/bourseweb/Negociation-Historique.aspx?Cat=24&IdLink=302"

XML_FILE = 'Telechargement-Histo-Valeur.aspx'

VOLUMES_COLS = ['seance', 'cours_cloture', 'cours_ajuste', 'evolution', 'quantite_echange', 'volume']

dico_valeurs = {
   '9000  ' : 'DOUJA PROM ADDOHA',
   '11200 ' : 'ALLIANCES',
   '11700 ' : 'AFRIC INDUSTRIES SA',
   '12200 ' : 'AFMA',
   '6700  ' : 'AGMA',
   '6600  ' : 'ALUMINIUM DU MAROC',
   '27    ' : 'ARADEI CAPITAL',
   '3200  ' : 'AUTO HALL',
   '10300 ' : 'ATLANTASANAD',
   '8200  ' : 'ATTIJARIWAFA BANK',
   '3300  ' : 'BALIMA',
   '5100  ' : 'BMCI',
   '8000  ' : 'BCP',
   '1100  ' : 'BANK OF AFRICA',
   '3900  ' : 'CENTRALE DANONE',
   '3600  ' : 'CDM',
   '3100  ' : 'CIH',
   '4000  ' : 'CIMENTS DU MAROC',
   '11000 ' : 'MINIERE TOUISSIT',
   '9200  ' : 'COLORADO',
   '8900  ' : 'CARTIER SAADA',
   '4100  ' : 'COSUMAR',
   '2200  ' : 'CTM',
   '10900 ' : 'DELTA HOLDING',
   '4200  ' : 'DIAC SALAF',
   '10800 ' : 'DELATTRE LEVIVIER MAROC',
   '8500  ' : 'DARI COUSPATE',
   '9700  ' : 'DISWAY',
   '2300  ' : 'EQDOM',
   '9300  ' : 'FENIE BROSSETTE',
   '7100  ' : 'AFRIQUIA GAZ',
   '9600  ' : 'HPS',
   '8001  ' : 'ITISSALAT AL-MAGHRIB',
   '7600  ' : 'IB MAROC.COM',
   '12    ' : 'IMMORENTE INVEST',
   '9500  ' : 'INVOLYS',
   '11600 ' : 'JET CONTRACTORS',
   '11100 ' : 'LABEL VIE',
   '4800  ' : 'LESIEUR CRISTAL',
   '3800  ' : 'LAFARGEHOLCIM MAR',
   '8600  ' : 'LYDEC',
   '10000 ' : 'M2M Group',
   '1600  ' : 'MAGHREBAIL',
   '6500  ' : 'MED PAPER',
   '10600 ' : 'MICRODATA',
   '2500  ' : 'MAROC LEASING',
   '7300  ' : 'MANAGEM',
   '7200  ' : 'MAGHREB OXYGENE',
   '12300 ' : 'SODEP-Marsa Maroc',
   '21    ' : 'MUTANDIS SCA',
   '7000  ' : 'AUTO NEJMA',
   '7400  ' : 'NEXANS MAROC',
   '11300 ' : 'ENNAKL',
   '5200  ' : 'OULMES',
   '9900  ' : 'PROMOPHARM S.A.',
   '12000 ' : 'RES DAR SAADA',
   '5300  ' : 'REBAB COMPANY',
   '8700  ' : 'RISMA',
   '11800 ' : 'S.M MONETIQUE',
   '11400 ' : 'SAHAM ASSURANCE',
   '6800  ' : 'SAMIR',
   '2000  ' : 'SOCIETE DES BOISSONS DU MAROC',
   '1300  ' : 'SONASID',
   '10700 ' : 'SALAFIN',
   '1500  ' : 'SMI',
   '10400 ' : 'STOKVIS NORD AFRIQUE',
   '10500 ' : 'SNEP',
   '9800  ' : 'SOTHEMA',
   '9400  ' : 'REALISATIONS MECANIQUES',
   '11500 ' : 'STROC INDUSTRIE',
   '29    ' : 'TGCC S.A',
   '10100 ' : 'TIMAR',
   '12100 ' : 'TOTAL MAROC',
   '11900 ' : 'TAQA MOROCCO',
   '7500  ' : 'UNIMER',
   '6400  ' : 'WAFA ASSURANCE',
   '5800  ' : 'ZELLIDJA S.A'
}

dbs = dbsession()

def extract_volume_content(act_mad):
    os.chdir('/Users/ysn/Projects/kanyon/data/masi_files/')

    xml_data = open(XML_FILE,'r+', encoding ='utf-8')
    data = xml_data.read()

    data = data.encode('ascii','ignore').decode('utf-8','ignore')
    soup = BeautifulSoup(data, 'html.parser')
    rows = soup.find_all('tr')

    l_res = []
    for row in rows[1:]:
        cols=row.find_all('td')
        cols=[x.text.strip() for x in cols]
        l_res.append(cols)

    df = pd.DataFrame(l_res)

    df.columns = VOLUMES_COLS
    df.insert(1, 'name', dico_valeurs[act_mad])
    df['seance'] = df['seance'].apply(lambda x: to_date(x))
    df['cours_cloture'] = df['cours_cloture'].apply(lambda x: float(x.replace(',', '.')))
    df['cours_ajuste'] = df['cours_ajuste'].apply(lambda x: float(x.replace(',', '.')))
    df['evolution'] = df['evolution'].apply(lambda x: float(x.replace(',', '.')))
    df['quantite_echange'] = df['quantite_echange'].apply(lambda x: float(x))
    df['volume'] = df['volume'].apply(lambda x: float(x.replace(',', '.')))
    os.remove(XML_FILE)
    return df

def extract_volume(act_mad, d_i, d_f):
    """
    params :
    act_mad : libelle actions
    d_i : date initiale
    d_f : date finale
    """
    ## test if file exist
    #path = '/Users/ysn/Desktop/chromedriver'

    chromeOptions = webdriver.ChromeOptions()
    chromeOptions.add_experimental_option('prefs', PREFS)
    chromeOptions.add_argument("--headless")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options = chromeOptions)
    driver.get(URL_VOLUMES)

    elem = driver.find_element_by_xpath('//*[@id="HistoriqueNegociation1_HistValeur1_RBSearchDate"]')
    date_ini = driver.find_element_by_xpath('//*[@id="HistoriqueNegociation1_HistValeur1_DateTimeControl1_TBCalendar"]')
    date_fini = driver.find_element_by_xpath('//*[@id="HistoriqueNegociation1_HistValeur1_DateTimeControl2_TBCalendar"]')
    downlo = driver.find_element_by_xpath('//*[@id="HistoriqueNegociation1_HistValeur1_LinkButton1"]')
    listo = driver.find_element_by_xpath('//*[@id="HistoriqueNegociation1_HistValeur1_DDValeur"]')
    valid = driver.find_element_by_xpath('//*[@id="HistoriqueNegociation1_HistValeur1_ImageButton1"]')

    driver.execute_script("arguments[0].click()", elem)
    driver.execute_script(f"arguments[0].value ='{act_mad}'", listo)
    driver.execute_script(f"arguments[0].value='{d_i}'", date_ini)
    driver.execute_script(f"arguments[0].value='{d_f}'", date_fini)
    driver.execute_script("arguments[0].click()", valid)
    driver.execute_script("arguments[0].click()", downlo)
    time.sleep(3)
    dres = extract_volume_content(act_mad)
    driver.close()
    return dres

def extract_implement_volume(d_i, d_f):
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
        objectif: volume to db
    '''


    # --- base --- #
    quer_masi_vol = dbs.query(MasiVolume)
    dquer_masi_vol = pd.read_sql(quer_masi_vol.statement, quer_masi_vol.session.bind)
    dquer_masi_vol_rt = dquer_masi_vol.drop(columns='id')
    dquer_masi_vol_rts = dquer_masi_vol_rt.sort_values(['seance'])
    dquer_masi_vol_rtsd = dquer_masi_vol_rts.drop_duplicates(['seance','name','cours_cloture','cours_ajuste','evolution', 'quantite_echange', 'volume']).sort_values(['seance'])

    first_date_indb = dquer_masi_vol_rtsd.seance[0]
    last_date_indb = dquer_masi_vol_rtsd.seance.tail(1).values[0]

    list_dates_masi_volume = list(dquer_masi_vol_rtsd.seance)


    if (to_date(d_i) in list_dates_masi_volume) and (to_date(d_f) in list_dates_masi_volume):
        print(f'volumes_existants du {d_i} of {d_f}',
            f'premiere date:{first_date_indb}',
            f'dernier date:{last_date_indb}'
            )

    elif (to_date(d_i) in list_dates_masi_volume) and (to_date(d_f) not in list_dates_masi_volume) and (to_date(d_f) > last_date_indb):

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
            for el in dico_valeurs.keys():
                print(el)
                try:

                    dres = extract_volume(el, first_list_dates_res, last_list_dates_res)
                    dres.to_sql('masi_volume', con = engine_lite, if_exists='append', index = False)

                    print(
                        f"Done import valeur : {el} -- du {first_list_dates_res} au {last_list_dates_res}"
                        )

                except:
                    print(f'probleme dimportation masi_volume {el}')

    else:
        for el in dico_valeurs.keys():
            try:
                dres = extract_volume(el, d_i, d_f)
                dres.to_sql('masi_volume', con = engine_lite, if_exists='append', index = False)

                print(
                    f"Done import indice : {el} -- du {d_i} au {d_f}",
                    f'premiere date:{first_date_indb}',
                    f'dernier date:{last_date_indb}'
                    )

            except:
                print(f'probleme dimportation {el}')
