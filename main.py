import log_parser.s3Log_parser as log_parser


if __name__ == '__main__':
    log_parser.download_log_file()
    log_parser.read_file()

