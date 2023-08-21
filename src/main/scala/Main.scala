object Main {
  def main(args: Array[String]): Unit = {
//    val inputFile = "/Users/luyang/Documents/project/nci_test/LUBM10.nt"
//    val outputDIR = "/Users/luyang/Documents/project/nci_test/"
//    val inputFile = "/Users/luyang/Documents/project/db/NCI_MAPPING/test.nt"
//    val outputDIR = "/Users/luyang/Documents/project/db/NCI_MAPPING/"
//    val inputFile = "/Users/luyang/Documents/project/db/NCI_INDEX/Benchmark/dataset/BeSEPPIgraph.nt"
//    val outputDIR = "/Users/luyang/Documents/project/db/NCI_INDEX/Benchmark/"
      val inputFile = "/Users/luyang/Documents/project/db/AiswcJAR/uobm1.nt"
      val outputDIR = "/Users/luyang/Documents/project/db/NCI_INDEX/uobm1/"
    Settings.loadUserSettings(inputFile, outputDIR)

    // inputfile outputDIR
//    Settings.loadUserSettings(args(0), args(1))
    DataMapping.dataMapping(Settings.inputFile, Settings.outputDIR)
  }
}
