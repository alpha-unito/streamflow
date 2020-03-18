cwlVersion: v1.1
class: ExpressionTool

inputs:
  inputArray:
    type:
      type: array
      items: [{type: array, items: File}]

outputs:
  flattenedArray:
    type: File[]

expression: >
  ${
    var flatArray= [];
    for (var i = 0; i < inputs.inputArray.length; i++) {
      for (var k = 0; k < inputs.inputArray[i].length; k++) {
        flatArray.push((inputs.inputArray[i])[k]);
      }
    }
    return { 'flattenedArray' : flatArray }
  }