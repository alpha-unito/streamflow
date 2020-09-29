cwlVersion: v1.1
class: ExpressionTool
requirements:
  InlineJavascriptRequirement: { }
inputs:
  worker: int
  files_array: File[]

outputs:
  directories:
    type: Directory[]

expression: >
  ${
    var directories = [];
    var listings = new Array(inputs.worker);  
    var current_listing = 0;

    for (var i = 0; i < listings.length; i++)
        listings[i] = [];

    for(var i = 0; i < inputs.files_array.length; i++){
        listings[current_listing].push(inputs.files_array[i]);
        current_listing = (current_listing + 1) % inputs.worker; 
    }

    for (var i = 0; i < listings.length; i++)
      directories.push( {"class": "Directory", "basename": "dir" + i.toString(), "listing": listings[i]});

    return {"directories": directories};
  }