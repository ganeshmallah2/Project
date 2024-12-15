import logging as log

def write_output(object, output_file, mode):
    """Writes the results to the specified output file."""
    try:
        with open(output_file, mode) as f:
            f.write(object + ' \n')
        log.info(f"Results written to {output_file}")
    except Exception as e:
        log.error(f"Error writing output: {e}")
        return e
