package bigdata

import groovy.json.JsonSlurper

class YelpUserToGraph {

    static main(args) {
        JsonSlurper jsonSlurper = new JsonSlurper()
        def jsonObj = jsonSlurper.parseText('{"yelping_since": "2011-12", "votes": {"funny": 0, "useful": 0, "cool": 0}, "review_count": 1, "name": "Matthew", "user_id": "MWhR9LvOdRbqtu1I_DRFBg", "friends": ["8Y2EN4XNNhnwssuPb31sJg", "A1jPleJ99kXZ3t9wQ3np-g", "resYiOoGkQg6q0qgtj_1GA", "skl1OnkjMqD4GdFpVhU88Q", "3Ss2aqrSoO7WbxE2GcLlfQ", "SGy3JDbhtzDTTBO7unqQxg", "Aez4Y1F0m2ucIfmfzPZfjw", "fT4jx_9cRWyKFROxv_MIrw", "QmZOAYM7ITbTdkTn6ay_ag", "_l18_te9ifeArhNdCfFy1g", "H6ovBilqI3cSunx27XbPUg", "A8pmiEiiWjXci_oeigLNZQ", "fdSfg45S-P-FcVF-VIUOew"], "fans": 0, "average_stars": 5.0, "type": "user", "compliments": {}, "elite": []}')
        println jsonObj?.elite?.size
    }
    
}
