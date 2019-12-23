from LoggerWrapper import LoggerWrapper
import importlib

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

def execute(input, *args):
  logger.debug("Executing pipelines with args input: " + str(input))
  output = None
  for arg in args:
    output = arg(input)
    logger.debug("Yield output " + str(output) + " from " + str(arg) + " pipeline")
    input = output
    yield output
  
  # end for

# end def

def execute(*args, **kwargs):
  logger.debug("Executing pipelines with kwargs input")
  input = kwargs
  output = None
  print(input)
  for arg in args:
    output = arg(**input)
    logger.debug("Yield output " + str(output) + " from " + str(arg) + " pipeline")
    input = output
    yield output

  # end for

# end def

def get_execution_result(results):
  output = None
  iterator = iter(results)
  done_looping = False
  while not done_looping:
    try:
      result = next(iterator)
    # end try
    
    except StopIteration:
      done_looping = True
      return output
    # end exception
    
    else:
      logger.debug("Retrieve result " + str(result) + " from execution stack trace")
      output = result
    # end case
  
  # end while

def import_module(module_name):
  tmp = module_name.split(".")
  module = ".".join(tmp[:len(tmp)-1])
  print("Module: ", module)
  funct = tmp[len(tmp)-1]
  print("Function: ", funct)
  importlib.import_module(module)
  print("Module ", module, " imported")
  return module
# end def

# end def
