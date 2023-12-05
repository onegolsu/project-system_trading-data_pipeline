# Data-Pipline

## Based on Airflow

```
.
├── dags
│   ├── system_trading
│   │   ├── dart.py
│   │   ├── fdr.py
│   │   └── pykrx.py
├── plugins
│   └── system_trading
│       ├── __init__.py
│       ├── api_loader
│       │   ├── __init__.py
│       │   ├── dart.py
│       │   ├── fdr.py
│       │   ├── private.py
│       │   └── pykrx.py
│       └── controller
│           ├── __init__.py
│           ├── db.py
│           └── private.py

~/airflow (tree)
```

### Dart
![image](./README_ASSETS/dart.png)
- dart
    - general info
    - fundamental data
### Finance_Data_Reader
![image](./README_ASSETS/fdr.png)
- fdr
    - general info
    - ohlcv

### pykrx
![image](./README_ASSETS/pykrx.png)
- pykrx
    - trader 
        - foreign trade
        - corporation trade
        - individual trade