assuming that you want to scrape metrics exposed on port 8090 in the prometheus format the following command should be executed
```
pmm-admin add external --listen-port=8090
```

you can pass additional parameters if required, please consider check the help
