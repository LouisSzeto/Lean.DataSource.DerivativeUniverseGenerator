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

using QuantConnect.Indicators;
using QuantConnect.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace QuantConnect.DataSource.OptionsUniverseGenerator
{
    public class OptionAdditionalFieldGenerator : DerivativeUniverseGenerator.AdditionalFieldGenerator
    {
        private const string _impliedVolHeader = "implied_volatility";
        private const string _deltaHeader = "delta";
        private const string _sidHeader = "#symbol_id";
        private const string _tickerHeader = "symbol_value";
        private const string _priceHeader = "close";
        private Dictionary<string, Dictionary<DateTime, decimal>> _iv30s = new();

        public OptionAdditionalFieldGenerator(DateTime processingDate, string rootPath)
            : base(processingDate, rootPath)
        {
        }

        public override bool Run()
        {
            Log.Trace($"OptionAdditionalFieldGenerator.Run(): Processing additional fields for date {_processingDate:yyyy-MM-dd}");

            // per symbol
            try
            {
                foreach (var subFolder in Directory.GetDirectories(_rootPath))
                {
                    _iv30s[subFolder] = new();
                    var dateFile = Path.Combine(subFolder, $"{_processingDate:yyyyMMdd}.csv");
                    var symbol = subFolder.Split(Path.DirectorySeparatorChar)[^1].ToUpper();
                    if (!File.Exists(dateFile))
                    {
                        Log.Error($"OptionAdditionalFieldGenerator.Run(): no universe file found for {symbol} in {_processingDate:yyyy-MM-dd}");
                        return false;
                    }

                    CleanIV(dateFile);

                    var ivs = GetIvs(_processingDate, subFolder);
                    var additionalFields = new OptionAdditionalFields();
                    additionalFields.Update(ivs);

                    WriteToCsv(dateFile, additionalFields);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex,
                    $"OptionAdditionalFieldGenerator.Run(): Error processing addtional fields for date {_processingDate:yyyy-MM-dd}");
                return false;
            }
            return true;
        }

        private void CleanIV(string csvPath)
        {
            var lines = File.ReadAllLines(csvPath)
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .ToList();
            var headers = lines[0].Split(',');
            var ivIndex = Array.IndexOf(headers, _impliedVolHeader);
            var sidIndex = Array.IndexOf(headers, _sidHeader);
            var tickerIndex = Array.IndexOf(headers, _tickerHeader);
            var priceIndex = Array.IndexOf(headers, _priceHeader);

            if (ivIndex == -1 || sidIndex == -1 || tickerIndex == -1 || priceIndex == -1)
            {
                return;
            }

            var underlyingClose = decimal.Parse(lines[1].Split(',')[priceIndex]);
            // Skip underlying row
            var data = lines.Skip(2)
                .Select(line =>
                {
                    var values = line.Split(',');
                    var symbol = new Symbol(SecurityIdentifier.Parse(values[sidIndex]), values[tickerIndex]);
                    var iv = decimal.Parse(values[ivIndex]);

                    return (Symbol: symbol, ImpliedVolatility: iv);
                }).Where(x => x.ImpliedVolatility > 0m)
                .ToList();
            var interpolation = new IvInterpolation(underlyingClose, _processingDate, data);

            using (var writer = new StreamWriter(csvPath))
            {
                int count = 0;

                foreach (var row in lines)
                {
                    var items = row.Split(',').ToList();

                    if (count >= 2 && decimal.Parse(items[ivIndex]) == 0m)
                    {
                        var symbol = new Symbol(SecurityIdentifier.Parse(items[sidIndex]), items[tickerIndex]);
                        var newIv = Convert.ToDecimal(interpolation.GetInterpolatedIv(symbol.ID.StrikePrice, symbol.ID.Date));
                        items[ivIndex] = $"{newIv}";

                        var greeks = interpolation.GetUpdatedGreeks(symbol, newIv, OptionPricingModelType.BlackScholes, OptionPricingModelType.BlackScholes);
                        items[ivIndex + 1] = $"{greeks.Delta}";
                        items[ivIndex + 2] = $"{greeks.Gamma}";
                        // validate the direction of some greeks
                        items[ivIndex + 3] = $"{Math.Max(0m, greeks.Vega)}";
                        items[ivIndex + 4] = $"{Math.Min(0m, greeks.Theta)}";
                        items[ivIndex + 5] = $"{Math.Max(0m, greeks.Rho)}";
                    }

                    writer.WriteLine(string.Join(',', items));

                    count++;
                }
            }
        }

        private List<decimal> GetIvs(DateTime currentDateTime, string path)
        {
            // get i-year ATM IVs to calculate IV rank and percentile
            var lastYearFiles = Directory.EnumerateFiles(path, "*.csv")
                .AsParallel()
                .Where(file => DateTime.TryParseExact(Path.GetFileNameWithoutExtension(file), "yyyyMMdd",
                        CultureInfo.InvariantCulture, DateTimeStyles.None, out var fileDate)
                    && fileDate > currentDateTime.AddYears(-1)
                    && fileDate <= currentDateTime
                    && !_iv30s[path].ContainsKey(fileDate))
                .ToDictionary(
                    file => DateTime.ParseExact(Path.GetFileNameWithoutExtension(file), "yyyyMMdd",
                    CultureInfo.InvariantCulture, DateTimeStyles.None),
                    file => GetAtmIv(file)
                );
            _iv30s[path] = _iv30s[path].Concat(lastYearFiles)
                .ToDictionary(x => x.Key, x => x.Value);

            return _iv30s[path].Where(x => x.Key > currentDateTime.AddYears(-1) && x.Key <= currentDateTime)
                .OrderBy(x => x.Key)
                .Select(x => x.Value)
                .ToList();
        }

        private decimal GetAtmIv(string csvPath)
        {
            var lines = File.ReadAllLines(csvPath)
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .ToList();
            var headers = lines[0].Split(',');
            var deltaIndex = Array.IndexOf(headers, _deltaHeader);
            var ivIndex = Array.IndexOf(headers, _impliedVolHeader);
            var sidIndex = Array.IndexOf(headers, _sidHeader);
            var tickerIndex = Array.IndexOf(headers, _tickerHeader);

            if (deltaIndex == -1 || ivIndex == -1 || sidIndex == -1 || tickerIndex == -1)
            {
                return -1m;
            }

            // Skip underlying row
            var filtered = lines.Skip(2)
                .Select(line =>
                {
                    var values = line.Split(',');
                    var symbol = new Symbol(SecurityIdentifier.Parse(values[sidIndex]), values[tickerIndex]);
                    var delta = decimal.Parse(values[deltaIndex]);
                    var iv = decimal.Parse(values[ivIndex]);
                    return (Expiry: symbol.ID.Date, Delta: delta, ImpliedVolatility: iv);
                })
                .Where(x => x.ImpliedVolatility != 0m)
                .ToList();
            if (filtered.Count == 0)
            {
                return -1m;
            }

            var expiries = filtered.Select(x => x.Expiry).ToList().Distinct();
            var currentDateTime = DateTime.ParseExact(Path.GetFileNameWithoutExtension(csvPath), "yyyyMMdd", 
                CultureInfo.InvariantCulture, DateTimeStyles.None);
            var day30 = currentDateTime.AddDays(30);
            var nearExpiry = expiries.Where(x => x <= day30).Max();
            var farExpiry = expiries.Where(x => x >= day30).Min();
            
            var nearIv = filtered.Where(x => x.Expiry == nearExpiry)
                .OrderBy(x => Math.Abs(x.Delta - 0.5m))
                .First()
                .ImpliedVolatility;
            if (nearExpiry == farExpiry)
            {
                return nearIv;
            }
            var farIv = filtered.Where(x => x.Expiry == farExpiry)
                .OrderBy(x => Math.Abs(x.Delta - 0.5m))
                .First()
                .ImpliedVolatility;
            // Linear interpolation
            return (nearIv * Convert.ToDecimal((farExpiry - day30).TotalDays) + farIv * Convert.ToDecimal((day30 - nearExpiry).TotalDays))
                / Convert.ToDecimal((farExpiry - nearExpiry).TotalDays);
        }
    }
}
