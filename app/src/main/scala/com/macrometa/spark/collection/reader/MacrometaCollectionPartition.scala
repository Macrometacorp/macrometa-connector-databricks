/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.reader

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaCollectionPartition(options: CaseInsensitiveStringMap)
    extends InputPartition {}
