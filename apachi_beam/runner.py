#--direct runner
'''python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts'''

#spark runner
'''python -m apache_beam.examples.wordcount --input /path/to/inputfile \
                                         --output /path/to/write/counts \
                                         --runner SparkRunner'''

#dataflow runner
'''# As part of the initial setup, install Google Cloud Platform specific extra components. Make sure you
# complete the setup steps at /documentation/runners/dataflow/#setup
pip install apache-beam[gcp]
python -m my_beam.py --input gs://dush-21/df/data1.csv --runner DataflowRunner --project jovial-totality-360004 --region us-centeral\
                                         --temp_location gs://dush-21/df/temp'''