const { KafkaClient } = require("../config/kafkaConfig")
const { KAFKA_GROUP_ID, KAFKA_TEST_TOPIC } = require("../config/index")
const FUNCTIONAL_LOCATION_AND_EQUIPMENT_MASTER = require("../src/service/FUNCTIONAL.LOCATION.AND.EQUIPMENT.MASTER");
const FUNCTIONAL_EQUIPMENT_MASTER = require("../src/service/FUNCTIONAL.EQUIPMENT.MASTER")
const FUNCTIONAL_MDM = require("../src/service/MATERIAL");

const consumer_TEST = KafkaClient.consumer({
    groupId: KAFKA_GROUP_ID,
    sessionTimeout: 60 * 1000
})

const run = async () => {

    //content
    await consumer_TEST.connect();

    //subscribe topic
    await consumer_TEST.subscribe({
        topics: [KAFKA_TEST_TOPIC],
        fromBeginning: false
    });

    try{
        const data_TEST = await consumer_TEST.describeGroup();
        console.log('Connection Test Topics: ', data_TEST)

        //consume
        await consumer_TEST.run({
            eachBatchAutoResolve: true,
            eachBatch: async ({
              batch,
              resolveOffset,
              heartbeat,
              commitOffsetsIfNecessary,
              uncommittedOffsets,
              isRunning,
              isStale,
              pause,
            }) => {
              console.log("consumer_TEST");
              for (let message of batch.messages) {
                console.log({
                  topic: batch.topic,
                  partition: batch.partition,
                  offset: message.offset,
                });
      
                try {
                  const dataFromKafka = JSON.parse(message.value.toString());
                  switch (batch.topic) {
                    case "SIT.CPF.FOOD.OMS.TEST.WORK.CENTER":
                      console.log("Calling FUNCTIONAL_EQUIPMENT_MASTER");
                      console.log("dataFromKafka : ", dataFromKafka);
                      await FUNCTIONAL_EQUIPMENT_MASTER.select(
                        dataFromKafka
                      ).catch((err) => {
                        console.log(
                          "error in FUNCTIONAL_EQUIPMENT_MASTER => ",
                          err
                        );
                      });
                      break;
                    default:
                      console.log("unknow topic :", topic);
                      break;
                  }
                } catch (error) {
                  console.log("Err commit", error.message);
                }
                resolveOffset(message.offset);
                await heartbeat();
              }
            },
          });
    }catch(err){
        
    }


}

run().catch(async (error) => {
    console.log("error3");
    console.error(error);
    try {
      await consumer_TEST.disconnect();
    } catch (e) {
      console.error("Failed to gracefully disconnect consumer", e);
    }
    process.exit(1);
  });