## eCrawler
<a href="https://github.com/hajipoor/eCrawler/raw/main/resources/overview.png"><img src="https://github.com/hajipoor/eCrawler/raw/main/resources/overview.png" align="center" height="89%" width="70%" ></a>

<a href="https://github.com/hajipoor/eCrawler/raw/main/resources/overview.pdf" target="_blank">Overview Architecture</a>
### Overview
For scalability and to make the most of parallelism, I separately developed independent modules which communicate through a shared memory.
 
- There is a program to handle how sites will be scraped (Spider)
- There is a program to perform the actual downloading (Downloader)
- There is a program to extract the text from the downloaded PDFs (Text Extractor)
- There is a program to parse information from the text (Date Extractor)

#### Requirements
- Installing requirements packages by running the following command
```python
$ conda create -n myenv python==3.7
$ conda activate myenv
$ pip3 install -r requirements.txt

```
- Redis, Please install Redis server by following link
[Getting started with Redis](https://redis.io/docs/getting-started/ "Getting started with Redis")

#### Run
- Redis
```bash
$ redis-server
```
- Spider
```python
 $ python spider.py  [--seeds_path "resources/seeds.json"] [--reset]
```
- Downloader
```python
 $ python downloader.py --download_path "resources/downloaded"
```
- Text Extractor.py
```python
 $ python text_extractor.py --download_path "resources/downloaded"
```
- Date Extractor.py
```python
 $ python date_extractor.py --saved_path "resources/json"
```

#### Scheduler
in Linux, Crontab can be used to schedule this program's execution automatically and ensure that the program will be resumed in case of any crash.
For example, the following command is executed every 10 minutes to ensure that one instance of crawler is running.

     $   */10 * * * * /usr/bin/flock -n /tmp/lock_spider_1 bash [full-path]/spider_1.sh



