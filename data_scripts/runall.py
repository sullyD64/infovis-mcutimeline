from data_scripts import logconfig, s1_parse, s2_scrape, s3_extract

if __name__ == "__main__":
    logconfig.config()
    s1_parse.main()
    s2_scrape.main()
    s3_extract.main()
