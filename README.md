# **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre analýzu dát z **MovieLens** datasetu. Cieľ daného projektu spočíva v výskume preferencií používateľov na základe informacií o ich hodnoteniach. Hľadanie súvislostí medzi údajmi používateľov a imi preferovanými filmamy. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

---
## **1. Úvod a popis zdrojových dát**
Ulohou semestrálneho projektu je analýza dát týkajúcich sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v používateľských preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta je možné najsť [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje osem hlavných tabuliek:
- `users`
- `ratings`
- `tags`
- `movies`
- `genres`
- `genres-movies`
- `occupations`
- `age_group`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### **1.1 Dátová architektúra**

### **ERD diagram**
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/DenBlade/MovieLens-ETL/blob/main/MovieLens_ERD.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:
- **`dim_movies`**: Obsahuje podrobné informácie o filmoch (názov, rok vydania).
- **`dim_users`**: Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, pohlavie, a povolanie.
- **`dim_tags`**: Obsahuje informácie o tagoch, ktorými používatelia označili filmy.
- **`dim_date`**: Zahrňuje informácie o dátumoch hodnotení (deň, tyždeň, mesiac, rok, štvrťrok).
- **`dim_time`**: Obsahuje podrobné časové údaje (hodiny, minuty, sekundy).

Na zniženie počtu duplicitných záznamov bolo rozhodnuté spraviť bridge table medzi **`dim_movies`** a **`dim_genres`**, čiže náš model obsahuje ešte 2 tabuľky:
- **`dim_genres`**: Obsahuje informácie o žánroch filmov.
- **`movies_genres_bridge`**: Napojenie medzi dimenziami **`dim_movies`** a **`dim_genres`**

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/DenBlade/MovieLens-ETL/blob/main/MovieLens_Star.png" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre AmazonBooks</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `temporary_stage`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE STAGE IF NOT EXISTS temporary_stage;
```
Aby dáta zo zdrojového datasetu ukladali sa rovnako, vytvorime vlastny **FILE FORMAT** na pracu s **CSV**:
```sql
CREATE OR REPLACE FILE FORMAT CSV_MovieLens_Format
    TYPE = CSV
    COMPRESSION = NONE
    SKIP_HEADER = 1
    FIELD_DELIMITER = '|'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
```

Do stage následne nahramé súbory, a ukažeme nás nový vytvorený **FILE FORMAT**. Dáta importujeme do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO users_staging
FROM @temporary_stage/users.csv
FILE_FORMAT = (FORMAT_NAME = CSV_MovieLens_Format) ON_ERROR='CONTINUE';
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý vynechá všetky problemové záznamy. Pokiaľ by sme chceli vynechať celý súbor pri dosiahnutí určitého počtu chyb, mohli by sme použiť `SKIP_FILE_<num>%`, ktorý nenačitá súbor pri prekročení nami zadefinovaného pomeru. Ale pre náš prípad nam stačí iba `CONTINUE`;

---
### **3.1 Transform (Transformácia dát)**

Pred tým ako zacať analyzovať dáta, musíme ich najprv transformovať, aby boli očistené a dalo sa s nimi ľahšie pracovať. Potrebujeme pripraviť dimenzie a faktovú tabuľku, ktoré umožnia buducú analýzu.

`dim_users` obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, zamestnania a zip-codu. Pretože mamé uchované vekové kategórie v pôvodnom datasete, nemusime toto transformovať, a len načítamé dáta s ohľadom na cudzie kľuče.
```sql
CREATE TABLE dim_users AS
SELECT
u.user_id AS dim_userId,
 a.name AS age_group,
 u.gender AS gender,
 u.zip_code AS zip_code,
 o.name AS occupation
FROM users_staging u JOIN age_group_staging a ON u.age_id = a.age_group_id
JOIN occupations_staging o ON u.occupation_id = o.occupation_id;
``` 
Takisto je správena aj dimenzia filmov, ktorá obsahuje informaciu o filmoch a ich rokoch vydania.

Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení filmov. Obsahuje odvodené údaje, ako sú deň, mesiac, rok, týždeň, deň v týždni a štvrťrok. Táto dimenzia je štruktúrovaná tak, aby umožňovala podrobné časové analýzy, ako sú trendy hodnotení podľa dní, mesiacov alebo rokov. 
```sql
CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE) DESC) AS dim_dateId, 
    CAST(rated_at AS DATE) AS date,                    
    DATE_PART(day, rated_at) AS day,                   
    DATE_PART(dow, rated_at) + 1 AS dayOfWeek,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS dayOfWeekAsString,
    DATE_PART(month, rated_at) AS month,              
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Oktober'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS monthAsString,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,               
    DATE_PART(quarter, rated_at) AS quarter           
FROM ratings_staging
GROUP BY date, day, dayOfWeek, month, year, week, quarter;
```

Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na iné dimenzie. Obsahuje kľúčové metriky, ako je hodnotenie používateľov a časový údaj.
```sql
CREATE TABLE fact_ratings AS
SELECT r.rating_id AS fact_ratingId,
r.rating AS rating,
r.rated_at AS datetime,
r.movie_id AS dim_movieId,
r.user_id AS dim_userId,
t.tag_id AS dim_tagId,
dd.dim_dateId AS dim_dateId,
dt.dim_timeId AS dim_timeId
FROM ratings_staging r JOIN users_staging u ON r.user_id = u.user_id
JOIN tags_staging t ON t.user_id = u.user_id
JOIN movies_staging m ON t.movie_id = m.movie_id
JOIN dim_time dt ON dt.time = CAST(r.rated_at AS TIME)
JOIN dim_date dd ON dd.date = CAST(r.rated_at AS DATE);
```

---
### **3.3 Load (Načítanie dát)**

Keď dimenzií a faktová tabuľka sa uspešne vytvorili, boli dáta nahraté do finálnej štruktúry. Potom nasledovalo odstranenie staging tabuliek, aby sa optimalizovalo využitie miesta úložiska:
```sql
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS movies_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. V dôsledku čoho sme očistili a reorganizovali dáta pre naše potreby. Výsledný model možeme využiť na analýzu preferencií a správania používateľov, takisto aj na vizualizáciu vysledkov analýzy.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základnú víziu o správaní používateľov a ich preferencie a hodnotenia. Na základe čoho sme schopní najsť nejaké tendencie v správaní používateľov a lepšie rozumieť ich preferenciam. 

<p align="center">
  <img src="https://github.com/DenBlade/MovieLens-ETL/blob/main/movielens_dashboard.jpg" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

---
### **Graf 1: Najlepšie hodnotené žánre**
Táto vizualizácia zobrazuje priemerné hodnotenie všetkých žánrov filmov v zostnupnom poradí. Umožňuje identifikovať najpopulárnejšie žánre medzi používateľmi. Zistíme napríklad, že najobľúbenejšie žánre našich používateľov sú `Film-Noir` a `Documentary`, čo môže znamenať, že v priemere naša služba sa používaje dospelymi ľudmi a mnoho hovorí o ich preferenciach. Tieto informácie môžu byť veľmi užitočné na odporúčanie filmov alebo na iné marketingové kampane.

```sql
SELECT g.genre_name AS movie_genre,
AVG(r.rating) AS average_rating
FROM fact_ratings r JOIN dim_movies m ON r.dim_movieId = m.dim_movieId
JOIN movies_genres_bridge mg ON mg.dim_movieId = m.dim_movieid
JOIN dim_genres g ON g.dim_genreId = mg.dim_genreId
GROUP BY movie_genre
ORDER BY average_rating DESC;
```
---
### **Graf 2: Rozdelenie hodnotení podľa vekových skupin používateľov**
Graf znázorňuje rozdiely v počte hodnotení medzi jednotlivými vekovými kategóriami. Ako aj predpokladalo sa, najviac ľudi práve v veku 25-34 používajú našu službu. A rozdiel medzi druhou za počtom skupiny(18-24) je väčší viac ako 2 krát. Táto vizualizácia ukazuje, že kampane lepšie zameriavať na mladých ľudi, pretože tie stanovia väčšiu časť aktivných používateľov. 

```sql
SELECT u.age_group AS age_group,
COUNT(r.fact_ratingid) AS count
FROM fact_ratings r JOIN dim_users u ON u.dim_userid = r.dim_userid
GROUP BY age_group
ORDER BY count DESC;
```
---
### **Graf 3: Najpoužívanejšie tagy používateľov**
Graf ukazuje, akými tagmi najčastejšie hodnotia filmy používatelia.  Najpoužívanejšie sú `atmospheric` a `classic`, čo znamená, že celkovo používateľia sú spokojní s filmami, ktoré naša služba poskytujé.
```sql
SELECT t.tags AS tags, COUNT(r.fact_ratingid) AS count
FROM fact_ratings r JOIN dim_tags t ON r.dim_tagid = t.dim_tagid
GROUP BY tags;
```
---
### **Graf 4: Rozdelenie hodnotení jednotlivých žánrov podľa pohlavia používateľov**
Tabuľka znázorňuje, ako sú priemerne hodnotenia žánrov sa rozlišujú podľa pohlavia používateľov. Ako vídime z grafu rozdiel naozaj je, ale nie je taký veľky, aby sme museli tuto informaciu zohľadňovať pri nejakých kampaniach. Kampane môžu byť efektívne zamerané na obe pohlavia bez potreby výrazného rozlišovania.

```sql
SELECT u.gender AS gender,
g.genre_name AS genre,
AVG(r.rating) AS rating
FROM fact_ratings r JOIN dim_users u ON u.dim_userid = r.dim_userid
JOIN dim_movies m ON m.dim_movieid = r.dim_movieid
JOIN movies_genres_bridge mg ON mg.dim_movieid = m.dim_movieid
JOIN dim_genres g ON g.dim_genreid = mg.dim_genreid
GROUP BY gender, genre; 
```
---
### **Graf 5: Počet hodnotení podľa jednotlivých mesiacov(2000)**
Tento graf poskytuje informácie o počte hodnotení podľa jednotlivých mesiacov. Čo može byť užitočným pri vyberaní vhodneho času pre naše kampane. Z údajov je vidieť, že mesiace, keď používatelia najčastejšie pozerajú filmy, sú `August` a `November`.

```sql
SELECT d.month AS month_int,
d.monthAsString AS month_name,
COUNT(r.fact_ratingid) AS count
FROM fact_ratings r JOIN dim_date d ON d.dim_dateid = r.dim_dateid
WHERE d.year = 2000
GROUP BY month_name, month_int
ORDER BY month_int;
```
---
### **Graf 6: Aktivita používateľov počas dňa**
Tento graf ukazuje, ako sa aktivita používateľov mení počas dňa. Z grafu vyplýva, že používatelia najviac sú aktívni ráno, a najmenšie aktivní dňom, čo môže súvisieť s pracovnými povinnosťami. Tieto informácie môžu pomôcť lepšie plánovať aktivity a rôzné kampane.
```sql
SELECT t.hour AS hour,
COUNT(r.fact_ratingid) AS count
FROM fact_ratings r
JOIN dim_time t ON r.dim_timeid = t.dim_timeid
GROUP BY hour ORDER BY HOUR;
```

Dashboard poskytuje široký pohľad na dáta a zodpovedá dôležité otázky týkajúce sa preferencií a správania používateľov. Vizualizácie dajú možnosť jednoducho uvidieť vysledok analýzy a vyčleniť dôležitú informaciu, ktorá môže byť využitá na optimalizáciu odporúčacích systémov, marketingových stratégií a iných aktivít.

---

**Autor:** Denys Klinkov
