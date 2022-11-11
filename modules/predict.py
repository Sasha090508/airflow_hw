import os
import sys
import json
import dill as dill
import pandas as pd


path = '/airflow_hw'
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)


def predict():
    with open(f'C:/Users/Александр/airflow_hw/data/models/cars_pipe_202211111234.pkl', 'rb') as file:
        model = dill.load(file)

    file_list = os.listdir('C:/Users/Александр/airflow_hw/data/test')

    predict_df = []

    for test_file in file_list:
        with open(os.path.join('C:/Users/Александр/airflow_hw/data/test', test_file), 'r') as json_file:
            test_dict = json.load(json_file)
        df = pd.DataFrame([test_dict])
        predict_df.append({'ID': test_file.replace(',json', ''), 'Prediction': model.predict(df)[0]})
    pred_df = pd.DataFrame(predict_df)
    pred_df.to_csv('C:/Users/Александр/airflow_hw/data/predictions/pre_cars.csv', index=False)



    #for filename in os.listdir('C:/Users/Александр/airflow_hw/data/test'):
    #    with open(f'C:/Users/Александр/airflow_hw/data/test' + filename, 'r') as f:
            #date = json.loads(f.read())
            #predict_df.append(date)

        #df = pd.DataFrame(predict_df)
        #y = model.predict(df)

        #df_result = pd.DataFrame(zip(df.id, y))
        #df_result.columns = [['id', 'predict']]

        #df_result.to_csv(f'{path}/data/predictions/pre_cars.csv', index=False)


if __name__ == '__main__':
    predict()
