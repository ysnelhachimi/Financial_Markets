import datetime as dt

import bs4

import requests
import pandas as pd
from sqlalchemy import (
    create_engine
)

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_loading import dbsession, engine_lite

from pyramido.pyrkanyon.pyrkanyon.models.helpers.p_factory import format, to_date

from pyramido.pyrkanyon.pyrkanyon.models.data.db.db_table import *

from pyramido.pyrkanyon.pyrkanyon.models.data.db_imports.import_masi.extract_masi_indice import get_days_imports_base


URL_PONDERATION = 'http://www.casablanca-bourse.com/bourseweb/Telechargement/Telechargement-Ponderation.aspx'

PONDERATION_COLS = [
    'seance',
    'code_isin',
    'libelle',
    'nombre_de_titre',
    'cours',
    'facteur_flottant',
    'facteur_de_plafonnement',
    'capi_flottante',
    'poids'
]


dbs = dbsession()


def extract_table_ponderation(soup: bs4.BeautifulSoup, cols: list) -> list:
    table_rows = soup.find_all('tr')
    rv = []

    for row in table_rows[1:]:

        values = [x.text for x in row.find_all('span')]

        dico_row = dict(zip(cols, values))

        rv.append(dico_row)

    return rv

def sanitize_table_ponderation(records: list) -> list:
    rv = []

    for el in records:
        el['seance'] = to_date(el['seance'])
        el['code_isin'] = el['code_isin'].strip().upper()
        el['libelle'] = el['libelle'].strip().upper()
        el['nombre_de_titre'] = int(sanitize_float(el['nombre_de_titre']))
        el['cours'] = sanitize_float(el['cours'])
        el['facteur_flottant'] = sanitize_float(el['facteur_flottant'])
        el['facteur_de_plafonnement'] = sanitize_float(el['facteur_de_plafonnement'])
        el['capi_flottante'] = sanitize_float(el['capi_flottante'])
        el['poids'] = sanitize_float(el['poids'])
        rv.append(el)

    return rv

def sanitize_float(sfloat: str) -> float:
    rv = float(sfloat.replace(',', '.'))
    return rv

def extract_masi_compo(debut, fin):
    deb, fin = to_date(debut), to_date(fin)
    #deb, fin = debut, fin
    params = {
        'Historique': 'Ponderation',
        'dateDeb': f"{deb:%d/%m/%Y}" + '00:00:00',
        'dateFin': f"{fin:%d/%m/%Y}" + '00:00:00',
        'var': 'MASI',
    }
    # url = f'http://www.casablanca-bourse.com/bourseweb/Telechargement/Telechargement-Ponderation.aspx?Historique=Ponderation&dateDeb={d:}00:00:00&dateFin={d:}00:00:00&var=MASI'
    resp = requests.get(URL_PONDERATION, params=params)
    a = resp.text
    soup = bs4.BeautifulSoup(a, 'html5lib') # If this line causes an error, run 'pip install html5lib' or install html5lib
    # print(soup.prettify())
    rows = extract_table_ponderation(soup, PONDERATION_COLS)
    rv = sanitize_table_ponderation(rows)

    return rv

def extract_implement_compo(d_i, d_f):
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
        objectif: composition to db
    '''

    # --- base --- #
    quer_masi_comp = dbs.query(MasiComposition)
    dquer_masi_comp = pd.read_sql(quer_masi_comp.statement, quer_masi_comp.session.bind)
    dquer_masi_comp_rt = dquer_masi_comp.drop(columns='id')
    dquer_masi_comp_rts = dquer_masi_comp_rt.sort_values(['seance'])
    dquer_masi_comp_rtsd = dquer_masi_comp_rts.drop_duplicates(['seance','code_isin','libelle','cours','capi_flottante','poids']).sort_values(['seance'])

    first_date_indb = dquer_masi_comp_rtsd.seance[0]
    last_date_indb = dquer_masi_comp_rtsd.seance[len(dquer_masi_comp_rtsd.seance)-1]

    list_dates_masicompo = list(dquer_masi_comp_rtsd.seance)


    if (to_date(d_i) in list_dates_masicompo) and (to_date(d_f) in list_dates_masicompo):
        print(f'composition_existante du {d_i} of {d_f}',
            f'premiere date:{first_date_indb}',
            f'dernier date:{last_date_indb}'
            )

    elif (to_date(d_i) in list_dates_masicompo) and (to_date(d_f) not in list_dates_masicompo) and (to_date(d_f) > last_date_indb):

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
            try:
                res = extract_masi_compo(str(first_list_dates_res), str(last_list_dates_res))
                df = pd.DataFrame(res)
                df.to_sql('masi_composition', con = engine_lite,if_exists='append', index = False)
                print("Done import masi_composition")
            except:
                print('probleme dimportation masi_composition')
    else:
        try:
            res = extract_masi_compo(d_i, d_f)
            df = pd.DataFrame(res)
            df.to_sql('masi_composition', con = engine_lite,if_exists='append', index = False)
            print("Done import masi_composition")
        except:
            print('probleme dimportation masi_composition')



#d = '11/06/2021'
#extract_implement_compo(d, d)
