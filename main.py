from fastapi import FastAPI, Query
import pandas as pd
from prophet import Prophet
import uvicorn

app = FastAPI()


def perform_forecast(months_limit):
    data = {
        "ds": [
            "2020-02-01", "2020-03-01", "2020-04-01", "2020-05-01", "2020-06-01",
            "2020-07-01", "2020-08-01", "2020-09-01", "2020-10-01", "2020-11-01",
            "2020-12-01", "2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01",
            "2021-05-01", "2021-06-01", "2021-07-01", "2021-08-01", "2021-09-01",
            "2021-10-01", "2021-11-01", "2021-12-01", "2022-01-01", "2022-02-01",
            "2022-03-01", "2022-04-01", "2022-06-01", "2022-07-01", "2022-08-01",
            "2022-09-01", "2022-10-01", "2022-11-01", "2022-12-01", "2023-01-01",
            "2023-02-01", "2023-03-01", "2023-04-01", "2023-05-01", "2023-06-01"
        ],
        "y": [
            8516.00, 8516.00, 8516.00, 100.00, 100.00, 14524.00, 7063.00, 7620.00, 8764.00,
            7776.00, 8696.00, 7770.00, 10035.00, 7440.00, 12076.00, 8514.00, 9365.00, 8840.00,
            9812.00, 8873.00, 8853.00, 9418.00, 10178.00, 8030.00, 8075.00, 8376.00, 5916.00,
            6709.00, 4727.00, 5155.00, 5250.00, 5126.00, 6174.00, 5945.00, 6402.00, 7865.00,
            8837.00, 6749.00, 5649.00, 6769.00
        ]
    }

    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Create a Prophet model
    model = Prophet()

    # Fit the model to the data
    model.fit(df)

    # Make predictions for the next `months_limit` months
    future = model.make_future_dataframe(periods=months_limit, freq='M')
    forecast = model.predict(future)

    # Extract the forecasted values for the next `months_limit` months
    forecast_values = forecast[['ds', 'yhat']].tail(months_limit-1).rename(columns={"ds": "Date", "yhat": "forecast"})

    return forecast_values


@app.post("/forecast")
def get_forecast(
                 months_limit: int = Query(..., description="Number of months for forecast")):
    forecast_values = perform_forecast(months_limit)
    return forecast_values.to_dict(orient='records')


# running the server
if __name__ == '__main__' : 
    uvicorn.run(app=app,host="127.0.0.1", port=5000, log_level="info")
