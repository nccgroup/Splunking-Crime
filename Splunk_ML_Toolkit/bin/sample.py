from __future__ import division
import sys
import StringIO
import csv
import random
import math
from util.chunk_util import read_chunk, write_chunk, log_and_warn, log_and_die
from util.param_util import parse_args, convert_params
from util.file_util import fix_line_ending

##################
# sample
##################
# [sample-command]
# syntax = sample (ratio=<float between 0 and 1>)? (count=<positive integer>)? (proportional=<name of numeric field> (inverse)?)? (partitions=<natural number greater than 1> (fieldname=<string>)?)? (seed=<number>)? (by <split_by_field>)?
# description = Randomly samples or partitions events. This command samples in one of three modes: ratio (returns an event with the given probability), count (returns exactly that number of events), and proportional (samples each event with probability specified by a field value). The fourth mode, partitioning, randomly divides events into the given number of partitions.
# shortdesc = Randomly samples or partitions events.
# comment1 = An example showing how to retrieve approximately 1% of the events, at random:
# example1 = sample ratio=0.01
# comment2 = An example showing how to retrieve exactly 20 events, at random, from each host:
# example2 = sample count=20 by host
# comment3 = An example showing how to sample to that each event is returned with probability determined by the value of some_field:
# example3 = sample proportional="some_field"
# comment4 = An example showing how to partition events into 7 groups, with the chosen group returned in a field called partition_number:
# example4 = sample partition=7 fieldname="partition_number"
# usage = public
# tags = sample random sampling partition

# [sample-seed-option]
# syntax = seed=<number>
# description = The random seed. If unspecified, a pseudorandom value is used.

# [sample-ratio-option]
# syntax = ratio=<float between 0 and 1>
# description = The probability with which to sample.

# [sample-count-option]
# syntax = count=<positive integer>
# description = The number of randomly chosen events to return. If the sample count exceeds the total number of events in the search, all events will be returned.

# [sample-by-option]
# syntax = by <split_by_field>
# description = When in count mode with parameter k, adding a split-by clause will return exactly k events for each value of the specified field, unless there are j < k events for some values, in which case all j events will be included in the results.

# [sample-proportional-option]
# syntax = proportional=<name of numeric field>
# description = The field to use for determining the sampling probability of each event.

# [sample-inverse-option]
# syntax = inverse
# description = In the proportional sampling mode, samples with one minus the probability specified in the proportional field.

# [sample-partitions-option]
# syntax = partitions=<natural number greater than 1>
# description = The number of partitions to create.

# [sample-fieldname-option]
# syntax = fieldname=<string>
# description = The name of the field in which to store the partition number. Defaults to 'partition_number'.

fix_line_ending()
csv.field_size_limit(10485760)

DEBUG = True
CHUNK_SIZE = 50000
BY_VALUE_MAX = 10000


def debug(msg):
    if DEBUG:
        sys.stderr.write("\nDEBUG: " + msg + "\n")


def main():
    out_metadata = {}

    random_seed = None
    sample_mode = None  # ratio, count, proportional, partition
    sample_ratio = None
    sample_count = None
    sample_proportional_field = None
    sample_inverse = False
    sample_partitions = None
    sample_partition_fieldname = None
    sample_split_by_field = None

    # Phase 0: getinfo exchange

    metadata, body = read_chunk(sys.stdin)

    # parse args
    options = parse_args(metadata['searchinfo']['args'])

    if 'params' in options:
        try:
            params = convert_params(options.get('params', {}), ints=['seed', 'count', 'partitions'], floats=['ratio'],
                                    strs=['fieldname', 'proportional'], aliases={})
        except RuntimeError as e:
            log_and_die(out_metadata, str(e))

        if 'seed' in params:
            random_seed = params['seed']

            if random_seed < 0:
                log_and_die(out_metadata, "Random seed must not be negative.")
            else:
                random.seed(random_seed)
        else:
            random.seed()

        if 'ratio' in params:
            sample_mode = "ratio"

            sample_ratio = params['ratio']

            if sample_ratio < 0 or sample_ratio > 1:
                log_and_die(out_metadata, "Sampling ratio must be a valid probability (i.e., in the interval [0,1]).")

        if 'count' in params:
            if sample_mode is not None:
                log_and_die(out_metadata, "More than one sampling mode specified.")
            else:
                sample_mode = "count"

            sample_count = params['count']

            if sample_count < 1:
                log_and_die(out_metadata, "Sample count must be one or greater.")

        if 'proportional' in params:
            if sample_mode is not None:
                log_and_die(out_metadata, "More than one sampling mode specified.")
            else:
                sample_mode = "proportional"

            sample_proportional_field = params['proportional']

        if 'partitions' in params:
            if sample_mode is not None:
                log_and_die(out_metadata, "More than one sampling mode specified.")
            else:
                sample_mode = "partition"

            sample_partitions = params['partitions']

            if sample_partitions < 2:
                log_and_die(out_metadata, "Must specify two or more partitions.")

        if sample_mode == "partition":
            if 'fieldname' in params:
                sample_partition_fieldname = params['fieldname']
            else:
                sample_partition_fieldname = 'partition_number'

        if 'fieldname' in params and sample_mode != "partition":
            log_and_die(out_metadata, "Only partition mode supports the fieldname parameter.")

    if 'args' in options:
        args = options['args']
        for arg in args:
            try:
                sample_arg = float(arg)

                if sample_arg > 0 and sample_arg < 1:
                    if sample_mode is not None:
                        log_and_die(out_metadata, "More than one sampling mode specified.")
                    sample_ratio = sample_arg
                    sample_mode = 'ratio'
                elif sample_arg >= 1 and sample_arg.is_integer():
                    if sample_mode is not None:
                        log_and_die(out_metadata, "More than one sampling mode specified.")
                    sample_count = sample_arg
                    sample_mode = 'count'
                else:
                    log_and_die(out_metadata,
                                "Must specify either a number between 0 and 1, non-inclusive (sampling probability) or an integer greater than or equal to 1 (count of events).")

            except ValueError:
                if arg != 'inverse' and arg != 'fieldname':
                    log_and_die(out_metadata, "Unrecognized argument: %s" % arg)

        if 'inverse' in args:
            if sample_mode == 'proportional':
                sample_inverse = True
            else:
                log_and_die(out_metadata, "Only proportional mode supports the inverse parameter.")

    if 'split_by' in options:
        if sample_mode == 'count':
            try:
                sample_split_by_field = options['split_by']

                if sample_split_by_field == "":
                    log_and_die(out_metadata, "Split-by field name is an empty string.")

            except ValueError:
                log_and_die(out_metadata, "Failed to parse split-by clause.")
        else:
            log_and_die(out_metadata, "A by clause can only be used in count mode.")

    if sample_mode == 'count':
        capdata = {'type': 'events'}
    else:
        capdata = {'type': 'stateful'}

    write_chunk(sys.stdout, capdata, '')

    # need to buffer events for all modes because we need all the field names
    if sample_split_by_field is None:
        event_reservoir = []
    else:
        event_reservoir = {}
        per_by_value_index = {}

    global_index = 0
    missing_split_by_field = 0

    # Phase 1: sample the events as they come in

    while True:
        ret = read_chunk(sys.stdin)
        if not ret:
            break
        metadata, body = ret

        out_metadata = {}
        out_metadata['finished'] = False
        outbuf = StringIO.StringIO()

        field_names = set()
        last_index = 0

        reader = csv.DictReader(body.splitlines(), dialect='excel')
        for index, record in enumerate(reader):

            # RATIO MODE
            if sample_mode == "ratio":
                if random.random() <= sample_ratio:
                    event_reservoir.append(record)

            # COUNT MODE
            # Uses reservoir sampling: https://en.wikipedia.org/wiki/Reservoir_sampling#Example_implementation
            elif sample_mode == "count":
                gindex = index + global_index
                if sample_split_by_field is not None:
                    if sample_split_by_field in record and record[sample_split_by_field] != "":
                        by_value = record[sample_split_by_field]
                    else:
                        by_value = 'NULL_BY_VALUE'
                        missing_split_by_field += 1

                    eres = event_reservoir.setdefault(by_value, [])
                    gindex = per_by_value_index.get(by_value, 0)

                    if len(per_by_value_index) > BY_VALUE_MAX:
                        log_and_die(out_metadata, "Too many values (> %d) for split-by field %s." % (
                        BY_VALUE_MAX, sample_split_by_field))

                    per_by_value_index[by_value] = gindex + 1
                else:
                    eres = event_reservoir

                if gindex < sample_count:
                    eres.append({'gindex': gindex, 'record': record})
                else:
                    r = random.randint(0, gindex)
                    if r < sample_count:
                        eres[r] = {'gindex': gindex, 'record': record}

            # PROPORTIONAL MODE
            elif sample_mode == "proportional":
                if sample_proportional_field not in record:
                    log_and_die(out_metadata,
                                "The specified field for proportional sampling does not exist: %s" % sample_partition_fieldname)

                try:
                    sample_proportional_val = float(record[sample_proportional_field])
                except ValueError:
                    log_and_die(out_metadata,
                                "The specified field for proportional sampling (%s) contains a non-numeric value: %s." % (
                                sample_proportional_field, record[sample_proportional_field]))

                if sample_proportional_val < 0 or sample_proportional_val > 1:
                    log_and_die(out_metadata,
                                "The field to use for proportional sampling must be a valid probability (i.e., between 0 and 1). Received %f." % sample_proportional_val)

                if sample_inverse:
                    sample_proportional_val = 1 - sample_proportional_val

                if random.random() <= sample_proportional_val:
                    event_reservoir.append(record)

            # PARTITION MODE
            elif sample_mode == "partition":
                p = random.randint(0, sample_partitions - 1)

                if sample_partition_fieldname is None:
                    sample_partition_fieldname = 'partition_number'

                if sample_partition_fieldname in record:
                    log_and_die(out_metadata,
                                "The specified field name for the partition already exists: %s" % sample_partition_fieldname)
                else:
                    record[sample_partition_fieldname] = p
                    event_reservoir.append(record)
            else:
                log_and_die(out_metadata, "Invalid sampling mode specified: %s" % sample_mode)

            last_index = index
            # we do this at the end so any added fields are included
            field_names = field_names.union(set(record.keys()))

        # finished reading the chunk; do any per-chunk actions
        global_index = global_index + last_index + 1

        if sample_mode != 'count':
            writer = csv.DictWriter(outbuf, fieldnames=list(field_names), dialect='excel', extrasaction='ignore')
            writer.writeheader()

            for event in event_reservoir:
                writer.writerow(event)

            write_chunk(sys.stdout, out_metadata, outbuf.getvalue())
            event_reservoir = []
            field_names = set()
        else:
            write_chunk(sys.stdout, {"finished": False}, '')

        if metadata.get('finished', False):
            break

    # Phase 2: output (count mode) and wrap-up

    if sample_mode == 'count':
        if sample_split_by_field is not None:
            merged_event_reservoir = []

            for by_value in event_reservoir:
                merged_event_reservoir.extend(event_reservoir[by_value])

            event_reservoir = sorted(merged_event_reservoir, key=lambda val: val['gindex'])
        else:
            event_reservoir = sorted(event_reservoir, key=lambda val: val['gindex'])

        # loop over CHUNK_SIZE slices of event_reservoir
        num_chunks = int(math.ceil(len(event_reservoir) / float(CHUNK_SIZE)))

        for i_chunk in range(num_chunks):
            if not read_chunk(sys.stdin):
                break

            outbuf = StringIO.StringIO()
            writer = csv.DictWriter(outbuf, fieldnames=list(field_names), dialect='excel', extrasaction='ignore')
            writer.writeheader()

            for val in event_reservoir[i_chunk * CHUNK_SIZE: i_chunk * CHUNK_SIZE + CHUNK_SIZE]:
                writer.writerow(val['record'])

            write_chunk(sys.stdout, {"finished": False}, outbuf.getvalue())

    # we're done, so send final response to finish the session
    ret = read_chunk(sys.stdin)
    if ret:
        out_metadata = {}
        out_metadata['finished'] = True

        if missing_split_by_field > 0:
            log_and_warn(out_metadata,
                         "%d events (out of %d) were missing the %s field and were sampled as though they all had the same value." % (
                         missing_split_by_field, global_index, sample_split_by_field))

        write_chunk(sys.stdout, out_metadata, '')


if __name__ == "__main__":
    main()
