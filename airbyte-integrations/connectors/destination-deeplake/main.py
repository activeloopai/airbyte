#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from destination_deeplake import DestinationDeeplake

if __name__ == "__main__":
    DestinationDeeplake().run(sys.argv[1:])
