object Main {
  def main(args: Array[String]): Unit = {
    val inputFile = "D:\\data\\LUBM10\\LUBM10.nt"
    val outputDIR = "D:\\data\\LUBM10\\output"
    Settings.loadUserSettings(inputFile, outputDIR)
    DataMapping.dataMapping(Settings.inputFile, Settings.outputDIR)
  }
}
