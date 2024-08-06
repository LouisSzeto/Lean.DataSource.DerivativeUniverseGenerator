/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2024 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Linq;
using MathNet.Numerics.Interpolation;
using static QuantConnect.DataSource.OptionsUniverseGenerator.OptionUniverseEntry;
using QuantConnect.Indicators;
using QuantConnect.Data;
using QuantConnect.Data.Market;

namespace QuantConnect.DataSource.OptionsUniverseGenerator
{
    public class IvCubicSplineInterpolation
    {
        private readonly static IRiskFreeInterestRateModel _interestRateProvider = new InterestRateProvider();
        private decimal _underlyingPrice;
        private DateTime _currentDate;
        private CubicSpline _cubicSpline;

        public IvCubicSplineInterpolation(decimal underlyingPrice, DateTime currentDate, IEnumerable<Symbol> symbols, IEnumerable<decimal> ivs)
        {
            _underlyingPrice = underlyingPrice;
            _currentDate = currentDate;

            var moneynesses = symbols.Select(x => GetMoneyness(x.ID.StrikePrice, x.ID.Date)).ToArray();
            var ivDoubles = ivs.Select(x => (double)x).ToArray();
            _cubicSpline = CubicSpline.InterpolateAkimaSorted(moneynesses, ivDoubles);
        }

        private double GetMoneyness(decimal strike, DateTime expiry)
        {
            var ttm = GetTimeTillMaturity(expiry);
            return Math.Log((double)(_underlyingPrice / strike)) / Math.Sqrt(ttm);
        }

        private double GetTimeTillMaturity(DateTime expiry)
        {
            return (expiry - _currentDate).TotalDays;
        }

        public decimal GetInterpolatedIv(decimal strike, DateTime expiry)
        {
            var moneyness = GetMoneyness(strike, expiry);
            var iv = _cubicSpline.Interpolate(moneyness);
            return Convert.ToDecimal(iv);
        }

        public Greeks GetUpdatedGreeks(Symbol option, decimal polatedIv)
        {
            var mirrorOption = OptionsUniverseGeneratorUtils.GetMirrorOptionSymbol(option);
            var greeks = new GreeksIndicators(option, mirrorOption);

            var dividendYieldModel = DividendYieldProvider.CreateForOption(option);
            var ttm = Convert.ToDecimal((option.ID.Date - _currentDate).TotalDays / 365d);
            var interest = _interestRateProvider.GetInterestRate(_currentDate);
            var dividend = dividendYieldModel.GetDividendYield(_currentDate);

            var optionPrice = 0m;
            var mirrorOptionPrice = 0m;
            if (option.ID.OptionStyle == OptionStyle.American)
            {
                optionPrice = OptionGreekIndicatorsHelper.ForwardTreeTheoreticalPrice(polatedIv, _underlyingPrice, option.ID.StrikePrice, ttm,
                    interest, dividend, option.ID.OptionRight);
                mirrorOptionPrice = OptionGreekIndicatorsHelper.ForwardTreeTheoreticalPrice(polatedIv, _underlyingPrice, mirrorOption.ID.StrikePrice, ttm,
                    interest, dividend, mirrorOption.ID.OptionRight);
            }
            else
            {
                optionPrice = OptionGreekIndicatorsHelper.BlackTheoreticalPrice(polatedIv, _underlyingPrice, option.ID.StrikePrice, ttm,
                    interest, dividend, option.ID.OptionRight);
                mirrorOptionPrice = OptionGreekIndicatorsHelper.BlackTheoreticalPrice(polatedIv, _underlyingPrice, mirrorOption.ID.StrikePrice, ttm,
                    interest, dividend, mirrorOption.ID.OptionRight);
            }

            greeks.Update(new IndicatorDataPoint(option.Underlying, _currentDate, _underlyingPrice));
            greeks.Update(new IndicatorDataPoint(option, _currentDate, optionPrice));
            greeks.Update(new IndicatorDataPoint(mirrorOption, _currentDate, mirrorOptionPrice));

            return greeks.GetGreeks();
        }
    }
}
