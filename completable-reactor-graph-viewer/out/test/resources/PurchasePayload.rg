{
  "version" : "v1_0_21",
  "payload" : {
    "payloadName" : "PurchasePayload",
    "payloadClass" : "ru.fix.completable.reactor.example.chain.PurchasePayload",
    "payloadDoc" : [ "Represent purchase request" ]
  },
  "startPoint" : {
    "coordinates" : {
      "x" : 680,
      "y" : 60
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 205
    },
    "builderPayloadSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 177
    },
    "processingItems" : [ {
      "type" : "PROCESSOR",
      "className" : "ServiceInfoProcessor",
      "name" : "gPurchaseServiceInfo"
    }, {
      "type" : "PROCESSOR",
      "className" : "UserProfileService",
      "name" : "gUserProfile"
    } ]
  },
  "processors" : [ {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "BankProcessor",
      "name" : "gBankPurchase"
    },
    "coordinates" : {
      "x" : 410,
      "y" : 440
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 206
    },
    "processorDoc" : [ "Withdraw money from Bank account ", "for given user." ],
    "handlerTitle" : "withdrawMoneyWithMinus",
    "handlerDoc" : [ "Withdraw money from account and print it to stdout", "If there is no money on account then withdrawing would be applied anyway", "and account balance became negative" ],
    "withHandlerSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 118
    }
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "NotificationProcessor",
      "name" : "gNotification"
    },
    "coordinates" : {
      "x" : 880,
      "y" : 430
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 207
    },
    "processorDoc" : [ "Send notifications to external system", "Notification about events like purchase.", "We do not need to wait result of notification action for building correct response.", "So we attach notification completable future to async task manager and continue without waiting for its result" ],
    "handlerTitle" : "sendPurchaseNotification",
    "handlerDoc" : [ "Send notification about purchase event" ],
    "withHandlerSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 87
    }
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "ServiceInfoProcessor",
      "name" : "gPurchaseServiceInfo"
    },
    "coordinates" : {
      "x" : 480,
      "y" : 120
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 208
    },
    "processorDoc" : [ "Service information processor load data for given service from database.", "Service could be not active. In that case purchase request will be denied." ],
    "handlerTitle" : "loadServiceInformation",
    "handlerDoc" : [ "Load service information from database." ],
    "withHandlerSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 150
    }
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "TransactionLogProcessor",
      "name" : "gTxLog"
    },
    "coordinates" : {
      "x" : 420,
      "y" : 650
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 209
    },
    "processorDoc" : [ "Store information about transactions.", "Each transaction have user id and money amount" ],
    "handlerTitle" : "logTransactioin",
    "handlerDoc" : null,
    "withHandlerSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 70
    }
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "UserLogProcessor",
      "name" : "gUserLog"
    },
    "coordinates" : {
      "x" : 680,
      "y" : 820
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 210
    },
    "processorDoc" : [ "Logs information about usres actions.", "Log contains user id" ],
    "handlerTitle" : "logAction",
    "handlerDoc" : null,
    "withHandlerSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 79
    }
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "UserProfileService",
      "name" : "gUserProfile"
    },
    "coordinates" : {
      "x" : 770,
      "y" : 120
    },
    "coordinatesSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 211
    },
    "processorDoc" : [ "UserProfileServices loads information about user from remote database.", "User profile contains information whether user is blocked or not." ],
    "handlerTitle" : "loadUserProfileById",
    "handlerDoc" : [ "Load user profile from database by user id." ],
    "withHandlerSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 43
    }
  } ],
  "subgraphs" : [ ],
  "mergePoints" : [ {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "UserProfileService",
      "name" : "gUserProfile"
    },
    "coordinates" : {
      "x" : 806,
      "y" : 201
    },
    "coordinatesSource" : null,
    "mergeSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 44
    },
    "mergerTitle" : "updateUserProfile",
    "mergerDocs" : [ ],
    "transitions" : [ {
      "mergeStatuses" : [ "STOP" ],
      "isOnAny" : false,
      "isComplete" : true,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : null,
      "completeCoordinates" : {
        "x" : 963,
        "y" : 258
      },
      "completeCoordinatesSource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 219
      },
      "completeSource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 182
      },
      "transitionsDoc" : [ {
        "mergeStatus" : "STOP",
        "docs" : [ "Stop processing" ]
      } ],
      "transitionOnAnySource" : null,
      "transitionOnStatusSource" : {
        "STOP" : {
          "className" : "ru.fix.completable.reactor.example.Configuration",
          "fileName" : "Configuration.java",
          "fileNameLine" : 182
        }
      },
      "mergeStatusSources" : {
        "STOP" : {
          "className" : "ru.fix.completable.reactor.example.MergeStatus",
          "fileName" : null,
          "fileNameLine" : null
        }
      },
      "complete" : true,
      "onAny" : false
    }, {
      "mergeStatuses" : [ "CONTINUE" ],
      "isOnAny" : false,
      "isComplete" : false,
      "mergeProcessingItem" : {
        "type" : "PROCESSOR",
        "className" : "ServiceInfoProcessor",
        "name" : "gPurchaseServiceInfo"
      },
      "handleByProcessingItem" : null,
      "completeCoordinates" : {
        "x" : 100,
        "y" : 100
      },
      "completeCoordinatesSource" : null,
      "completeSource" : null,
      "transitionsDoc" : [ {
        "mergeStatus" : "CONTINUE",
        "docs" : [ "Continue processing" ]
      } ],
      "transitionOnAnySource" : null,
      "transitionOnStatusSource" : {
        "CONTINUE" : {
          "className" : "ru.fix.completable.reactor.example.Configuration",
          "fileName" : "Configuration.java",
          "fileNameLine" : 183
        }
      },
      "mergeStatusSources" : {
        "CONTINUE" : {
          "className" : "ru.fix.completable.reactor.example.MergeStatus",
          "fileName" : null,
          "fileNameLine" : null
        }
      },
      "complete" : false,
      "onAny" : false
    } ]
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "ServiceInfoProcessor",
      "name" : "gPurchaseServiceInfo"
    },
    "coordinates" : {
      "x" : 640,
      "y" : 280
    },
    "coordinatesSource" : null,
    "mergeSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 151
    },
    "mergerTitle" : "updateServiceInfo",
    "mergerDocs" : [ ],
    "transitions" : [ {
      "mergeStatuses" : [ "WITHDRAWAL" ],
      "isOnAny" : false,
      "isComplete" : false,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : {
        "type" : "PROCESSOR",
        "className" : "BankProcessor",
        "name" : "gBankPurchase"
      },
      "completeCoordinates" : {
        "x" : 100,
        "y" : 100
      },
      "completeCoordinatesSource" : null,
      "completeSource" : null,
      "transitionsDoc" : [ {
        "mergeStatus" : "WITHDRAWAL",
        "docs" : [ "Withdraw money required" ]
      } ],
      "transitionOnAnySource" : null,
      "transitionOnStatusSource" : {
        "WITHDRAWAL" : {
          "className" : "ru.fix.completable.reactor.example.Configuration",
          "fileName" : "Configuration.java",
          "fileNameLine" : 187
        }
      },
      "mergeStatusSources" : {
        "WITHDRAWAL" : {
          "className" : "ru.fix.completable.reactor.example.MergeStatus",
          "fileName" : null,
          "fileNameLine" : null
        }
      },
      "complete" : false,
      "onAny" : false
    }, {
      "mergeStatuses" : [ "NO_WITHDRAWAL" ],
      "isOnAny" : false,
      "isComplete" : false,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : {
        "type" : "PROCESSOR",
        "className" : "UserLogProcessor",
        "name" : "gUserLog"
      },
      "completeCoordinates" : {
        "x" : 100,
        "y" : 100
      },
      "completeCoordinatesSource" : null,
      "completeSource" : null,
      "transitionsDoc" : [ {
        "mergeStatus" : "NO_WITHDRAWAL",
        "docs" : [ "No withdrawal required" ]
      } ],
      "transitionOnAnySource" : null,
      "transitionOnStatusSource" : {
        "NO_WITHDRAWAL" : {
          "className" : "ru.fix.completable.reactor.example.Configuration",
          "fileName" : "Configuration.java",
          "fileNameLine" : 188
        }
      },
      "mergeStatusSources" : {
        "NO_WITHDRAWAL" : {
          "className" : "ru.fix.completable.reactor.example.MergeStatus",
          "fileName" : null,
          "fileNameLine" : null
        }
      },
      "complete" : false,
      "onAny" : false
    }, {
      "mergeStatuses" : [ "NO_WITHDRAWAL" ],
      "isOnAny" : false,
      "isComplete" : false,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : {
        "type" : "PROCESSOR",
        "className" : "NotificationProcessor",
        "name" : "gNotification"
      },
      "completeCoordinates" : {
        "x" : 100,
        "y" : 100
      },
      "completeCoordinatesSource" : null,
      "completeSource" : null,
      "transitionsDoc" : [ {
        "mergeStatus" : "NO_WITHDRAWAL",
        "docs" : [ "No withdrawal required" ]
      } ],
      "transitionOnAnySource" : null,
      "transitionOnStatusSource" : {
        "NO_WITHDRAWAL" : {
          "className" : "ru.fix.completable.reactor.example.Configuration",
          "fileName" : "Configuration.java",
          "fileNameLine" : 189
        }
      },
      "mergeStatusSources" : {
        "NO_WITHDRAWAL" : {
          "className" : "ru.fix.completable.reactor.example.MergeStatus",
          "fileName" : null,
          "fileNameLine" : null
        }
      },
      "complete" : false,
      "onAny" : false
    }, {
      "mergeStatuses" : [ "STOP" ],
      "isOnAny" : false,
      "isComplete" : true,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : null,
      "completeCoordinates" : {
        "x" : 480,
        "y" : 310
      },
      "completeCoordinatesSource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 217
      },
      "completeSource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 190
      },
      "transitionsDoc" : [ {
        "mergeStatus" : "STOP",
        "docs" : [ "Stop processing" ]
      } ],
      "transitionOnAnySource" : null,
      "transitionOnStatusSource" : {
        "STOP" : {
          "className" : "ru.fix.completable.reactor.example.Configuration",
          "fileName" : "Configuration.java",
          "fileNameLine" : 190
        }
      },
      "mergeStatusSources" : {
        "STOP" : {
          "className" : "ru.fix.completable.reactor.example.MergeStatus",
          "fileName" : null,
          "fileNameLine" : null
        }
      },
      "complete" : true,
      "onAny" : false
    } ]
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "BankProcessor",
      "name" : "gBankPurchase"
    },
    "coordinates" : {
      "x" : 480,
      "y" : 550
    },
    "coordinatesSource" : null,
    "mergeSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 119
    },
    "mergerTitle" : "checkWithdrawResult",
    "mergerDocs" : [ "Checks result of withdraw operation", "Sets new amount and withdrawal status in payload", "Stops in case if operation is failed" ],
    "transitions" : [ {
      "mergeStatuses" : [ ],
      "isOnAny" : true,
      "isComplete" : false,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : {
        "type" : "PROCESSOR",
        "className" : "TransactionLogProcessor",
        "name" : "gTxLog"
      },
      "completeCoordinates" : {
        "x" : 100,
        "y" : 100
      },
      "completeCoordinatesSource" : null,
      "completeSource" : null,
      "transitionsDoc" : [ ],
      "transitionOnAnySource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 193
      },
      "transitionOnStatusSource" : { },
      "mergeStatusSources" : { },
      "complete" : false,
      "onAny" : true
    } ]
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "TransactionLogProcessor",
      "name" : "gTxLog"
    },
    "coordinates" : {
      "x" : 530,
      "y" : 770
    },
    "coordinatesSource" : null,
    "mergeSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 71
    },
    "mergerTitle" : null,
    "mergerDocs" : [ ],
    "transitions" : [ {
      "mergeStatuses" : [ ],
      "isOnAny" : true,
      "isComplete" : false,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : {
        "type" : "PROCESSOR",
        "className" : "UserLogProcessor",
        "name" : "gUserLog"
      },
      "completeCoordinates" : {
        "x" : 100,
        "y" : 100
      },
      "completeCoordinatesSource" : null,
      "completeSource" : null,
      "transitionsDoc" : [ ],
      "transitionOnAnySource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 197
      },
      "transitionOnStatusSource" : { },
      "mergeStatusSources" : { },
      "complete" : false,
      "onAny" : true
    } ]
  }, {
    "identity" : {
      "type" : "PROCESSOR",
      "className" : "UserLogProcessor",
      "name" : "gUserLog"
    },
    "coordinates" : {
      "x" : 760,
      "y" : 930
    },
    "coordinatesSource" : null,
    "mergeSource" : {
      "className" : "ru.fix.completable.reactor.example.Configuration",
      "fileName" : "Configuration.java",
      "fileNameLine" : 80
    },
    "mergerTitle" : null,
    "mergerDocs" : [ ],
    "transitions" : [ {
      "mergeStatuses" : [ ],
      "isOnAny" : true,
      "isComplete" : true,
      "mergeProcessingItem" : null,
      "handleByProcessingItem" : null,
      "completeCoordinates" : {
        "x" : 740,
        "y" : 1020
      },
      "completeCoordinatesSource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 218
      },
      "completeSource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 202
      },
      "transitionsDoc" : [ ],
      "transitionOnAnySource" : {
        "className" : "ru.fix.completable.reactor.example.Configuration",
        "fileName" : "Configuration.java",
        "fileNameLine" : 201
      },
      "transitionOnStatusSource" : { },
      "mergeStatusSources" : { },
      "complete" : true,
      "onAny" : true
    } ]
  } ],
  "serializationPointSource" : {
    "className" : "ru.fix.completable.reactor.example.ConfigurationTest",
    "fileName" : "ConfigurationTest.java",
    "fileNameLine" : 29
  },
  "coordinatesSource" : {
    "className" : "ru.fix.completable.reactor.example.Configuration",
    "fileName" : "Configuration.java",
    "fileNameLine" : 204
  },
  "buildGraphSource" : {
    "className" : "ru.fix.completable.reactor.example.Configuration",
    "fileName" : "Configuration.java",
    "fileNameLine" : 221
  }
}