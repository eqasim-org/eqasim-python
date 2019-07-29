import pandas as pd
import numpy as np
import os

def _calculate_bounds(df, modes, bin_size):
    bounds = {}

    for mode in modes:
        f = df["mode"] == mode

        values = df[f]["travel_time"].values
        weights = df[f]["weight"].values

        bins = []
        current_bin = []
        previous_value = np.nan

        for value in np.sort(values):
            if value == previous_value:
                bins[-1].append(value)
            else:
                current_bin.append(value)

            if len(current_bin) == bin_size:
                bins.append(current_bin)
                current_bin = []
                previous_value = value

        bounds[mode] = [max(b) for b in bins]

    return bounds

def _resample(cdf, factor):
    if factor >= 0.0:
        cdf = cdf * (1.0 + factor * np.arange(1, len(cdf) + 1) / len(cdf))
    else:
        cdf = cdf * (1.0 + abs(factor) - abs(factor) * np.arange(1, len(cdf) + 1) / len(cdf))

    cdf /= cdf[-1]
    return cdf

def create_input_distributions(df, output_path, bin_size = 400, samples = 1000, modes = None, resampling_factors = None):
    # Prepare data frame
    expected_columns = ["mode", "travel_time", "distance", "weight"]
    df = pd.DataFrame(df[expected_columns], copy = True)

    if modes is None:
        modes = df["mode"].unique()

    if resampling_factors is None:
        resampling_factors = { mode : 0.0 for mode in modes }

    for mode in modes:
        if not mode in resampling_factors:
            raise RuntimeError("No resampling factor give for mode %s" % mode)

    # Calculate bounds for bin size
    bounds = _calculate_bounds(df, modes, bin_size)

    if not os.path.isdir(output_path):
        raise RuntimeError("Directory does not exist: %s" % output_path)

    quantiles_path = "%s/quantiles.dat" % output_path
    distributions_path = "%s/distributions.dat" % output_path

    # Write distributions
    with open(quantiles_path, "w+") as quantiles_writer:
        with open(distributions_path, "w+") as distributions_writer:
            for mode in modes:
                f_mode = df["mode"] == mode

                quantiles_writer.write("%s;%s\n" % (mode, ";".join(map(str, bounds[mode]))))
                index = 0

                for lower_bound, upper_bound in zip([-np.inf] + bounds[mode], bounds[mode]):
                    f_bound = (df["travel_time"] > lower_bound) & (df["travel_time"] <= upper_bound)

                    # Set up distribution
                    values = df[f_mode & f_bound]["distance"].values
                    weights = df[f_mode & f_bound]["weight"].values

                    sorter = np.argsort(values)
                    cdf_values = values[sorter]

                    cdf = np.cumsum(weights[sorter])
                    cdf /= cdf[-1]

                    # Resampling
                    cdf = _resample(cdf, resampling_factors[mode])

                    # Generate samples
                    distribution = []

                    for k in range(samples):
                        u = np.random.random()
                        distribution.append(cdf_values[np.sum(cdf < u)])

                    distributions_writer.write("%s;%d;%s\n" % (mode, index,
                        ";".join(map(str, distribution))
                    ))

                    index += 1
