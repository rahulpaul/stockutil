import traceback
import numpy as np
import matplotlib.pyplot as plt

from typing import Dict, List
from collections import defaultdict

from .et_company_list_parser import PageData, DataPersistenceService, FinancialResultFeature, LinearFit


def get_industries_group_by_sector() -> Dict[str, List[str]]:
    result = defaultdict(set)
    sql = f"SELECT sector, industry FROM {DataPersistenceService.et_companies_data} ORDER BY sector"
    with DataPersistenceService() as service:
        data = service.execute(sql)
        for sector, industry in data:
            if (sector is not None) and (industry is not None):
                result[sector].add(industry)
        return result


def plot_result_series(data: PageData, feature: FinancialResultFeature) -> None:
    result = data.quarterly_results_df[feature.name]
    diff = result - result.shift(1)
    diff[diff.index[0]] = 0  # set the first value to 0
    if feature.higher_is_better:
        color_func = lambda x: 'red' if x < 0 else 'green'
    else:
        color_func = lambda x: 'green' if x < 0 else 'red'
    color = diff.apply(color_func)
    result.plot(kind='bar', color=color)


def compute_linear_fit_function(data: PageData, feature: FinancialResultFeature) -> LinearFit:
    column = feature.name
    df = data.quarterly_results_df
    if feature.name not in df.columns:
        raise ValueError
    y = df[column]
    if y.size < 10:
        raise ValueError
    x = np.arange(y.size)
    fit = np.polyfit(x, y, 1)
    fit_fn = np.poly1d(fit)
    predicted_y = fit_fn(x)
    if fit_fn.coeffs.size == 2:
        m, c = fit_fn.coeffs[0], fit_fn.coeffs[1]
    else:
        m, c = 0, 0
    return LinearFit(x, y, predicted_y, m, c)


def plot_result_with_linear_fit_line(data: PageData, feature: FinancialResultFeature) -> None:
    linear_fit = compute_linear_fit_function(data, feature)
    plt.plot(linear_fit.x, linear_fit.predicted_y, 'k-')
    plt.plot(linear_fit.x, linear_fit.y, 'go', ms=5)
    text = f'm={round(linear_fit.m, 2)}, c={round(linear_fit.c, 2)}'
    print(text)
    plt.text(x=0, y=np.max(linear_fit.y), s=text, fontsize=12)


def compute_and_save_linear_fit(interested_features: List[FinancialResultFeature],
                                company_data: PageData, service: DataPersistenceService):
    feature_to_linear_fit = {}
    for feature in interested_features:
        try:
            linear_fit = compute_linear_fit_function(company_data, feature)
            feature_to_linear_fit[feature] = linear_fit
        except ValueError:
            pass
        except:
            print(f"Error computing linear fit for feature {feature}, company {company_data.company_name}")
            traceback.print_exc()

    if feature_to_linear_fit:
        service.save_linear_fit(company_data, feature_to_linear_fit)
        print(company_data.company_name)


def compute_and_save_all_linear_fits():
    interested_features = [
        FinancialResultFeature.Sales,
        FinancialResultFeature.OperatingProfit,
        FinancialResultFeature.NetProfit,
        FinancialResultFeature.Ebita,
        FinancialResultFeature.Interest
    ]

    with DataPersistenceService() as service:
        service.create_tables()
        batch_size = 10
        offset = 0
        while True:
            company_list = service.fetch_et_companies(limit=batch_size, offset=offset)
            offset += batch_size

            for company in company_list:
                company_data = service.fetch_et_company_data(company)
                compute_and_save_linear_fit(interested_features, company_data, service)

            if len(company_list) < batch_size:
                print("\nCompleted")
                return


def test():
    import json
    from .et_company_list_parser import get_et_company_data
    page_data = get_et_company_data('Aikyam Intellectual Property Consultancy')
    print(json.dumps(page_data.to_json(), indent=4))
    interested_features = [
        FinancialResultFeature.Sales,
        FinancialResultFeature.OperatingProfit,
        FinancialResultFeature.NetProfit,
        FinancialResultFeature.Ebita,
        FinancialResultFeature.Interest
    ]
    if page_data:
        with DataPersistenceService() as service:
            compute_and_save_linear_fit(interested_features, page_data, service)


if __name__ == '__main__':
    # test()
    compute_and_save_all_linear_fits()

