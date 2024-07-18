from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
import time
import requests
from pyspark.sql.types import StringType

from pyspark.sql.functions import col, udf, current_date, current_timestamp

# from src.main.service.ProjectBetsService import ProjectBetsService

spark = SparkSession.builder \
    .appName("ProjectBets") \
    .getOrCreate()

def main(spark):

    start_time = time.time() # Captura el tiempo de inicio

    #Leer Json con Ligas
    data = spark.read.option(
        "multiline", "true"
    ).json("/Users/elvisandairalcantaramunoz/Documents/Andair_dev/ProjectBets/league/leagues.json")

    #Obtener un df con codigo y fecha inicio
    league_and_dates_df = data.select(
        col("code"),
        col("startDate")
    ).limit(1)

    def parse_html(html_string) :
        soup = BeautifulSoup(html_string, 'html.parser')
        table = soup.find('table', class_='table')
        if not table:
            return []
        rows = table.find_all('tr')
        result = []

        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 19:  # Asegurarse de que hay suficientes celdas
                rowData = {
                    'date': cells[0].get_text(strip=True),
                    'status': cells[1].get_text(strip=True),
                    'homeTeam': cells[2].get_text(strip=True),
                    'homeGoals': cells[3].get_text(strip=True),
                    'awayGoals': cells[4].get_text(strip=True),
                    'awayTeam': cells[5].get_text(strip=True),
                    'homeCorners': cells[7].get_text(strip=True),
                    'awayCorners': cells[8].get_text(strip=True),
                    'homeCornersHT': cells[9].get_text(strip=True),
                    'awayCornersHT': cells[10].get_text(strip=True),
                    'homeAttack': cells[15].get_text(strip=True),
                    'awayAttack': cells[16].get_text(strip=True),
                    'homeShots': cells[17].get_text(strip=True),
                    'awayShots': cells[18].get_text(strip=True),
                }
                result.append(rowData)

        return result

    def scrape_data(code):
        league_data = []
        max_retries = 2
        for page in range(1,10):
            retry = 0
            while retry <= max_retries:
                proxy_token = 'kg-p4rpRnss5hxODlgLPBg'

                url_proxy = f'https://api.crawlbase.com/?token={proxy_token}&url='

                url = f"{url_proxy}https://www.totalcorner.com/league/view/{code}/page:{page}?copy=yes"

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Connection': 'keep-alive'
                }
                print(f"scrapeando page : {url}")
                try:
                    response = requests.get(url, headers)
                    response.raise_for_status()  # Verifica si la solicitud fue exitosa
                    parsed_data = parse_html(response.text)
                    league_data.extend(parsed_data)
                    break

                    # print(f'registros = {len(parsed_data)}')
                    # print(f"scrapeado correcto a : {url}")
                    # return response.text  # Aquí puedes procesar los datos como necesites
                except requests.RequestException as e:
                    retry += 1
                    if retry > max_retries:
                        print(f"Error al obtener datos para {url}: {e}")
                    else:
                        print(f"Intento {retry} de {max_retries} para {url}")
                        time.sleep(5)  # Espera 5 segundos antes de reintentar
                # return None
        return league_data

    scrape_data_udf = udf(scrape_data, StringType())

    # # Aplica la UDF para agregar los datos scrapeados
    df_with_scraped_data = league_and_dates_df\
        .withColumn("scraped_data", scrape_data_udf(col('code')))

    df_with_scraped_data.show(truncate=False)

    df_with_scraped_data.write.mode('overwrite').json('/Users/elvisandairalcantaramunoz/Documents/Andair_dev/ProjectBets/src/output/')

    # total_corners_service = ProjectBetsService()






if __name__ == "__main__":
    main(spark)

