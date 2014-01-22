# Testing Pig

Apache Pig comes with its own testing framework called PigUnit. Allthough this framework provides sufficient capabilities for basic Pig Testing, it lacks some of the basic features like:

  - The ability to test scenarios with multiple inputs
  - Asserting output in a more fuzzy way. It has to match exactly
  - Unable to place DUMP statements inside scripts to test

To solve these issues a new testing framework has been called to life. It's name? NiftyPigUnit.

NiftyPigUnit is actually based on a copy of the PigUnit code from Apache Pig 0.11.0. We started first with extending the PigUnit class but ended up hitting walls because of the level of encapsulation used inside the original PigUnit code.

## A simple example

Let's take one of the most simple scripts we can imagine:

```
setA =
    LOAD 'mySource-1.csv'
    USING PigStorage(';')
    AS (
        field1: chararray,
        field2: chararray
    );

setB =
    LOAD 'mySource-2.csv'
    USING PigStorage(';')
    AS (
        field1: chararray,
        field2: chararray
    );

result =
    UNION ONSCHEMA setA, setB;

STORE result INTO 'output.csv' USING PigStorage(';');
```

Make sure you use "setA =" and not "setA="

What this script does is read two CSV files (mySource-1.csv and mySource-2.csv) and create a union of both. This actually means pasting one source after the other. Eventually the result is persisted using the standard PigStorage.

Allthough this is a very simple script, we still want to make sure it behaves as expected. To do so we will write a unit test:

```
public class SimpleUnionTest {
    private static final String PIG_SCRIPT = "src/test/resources/simpleUnion.pig";

    @Test
    @Category(TestCategories.PigTest.class)
    public void testTextInput() throws Exception{
        // -- initialize the pig testing class
        NiftyPigTest test = new NiftyPigTest(PIG_SCRIPT);

        // -- indicate which data we want to use for which pig aliases
        String[] setA = {
                "139380;AD210",
                "139380;AD2100"
        };
        test.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO",
        };
        test.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        test.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = test.validate(dataset("result").mode(DataSetValidator.ValidationMode.ByOrder).size(4)
                .add(tuple().field(string("SOHO")).field(string("SOHO")))
                .add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")))
                .add(tuple().field(string("139380")).field(string("AD210")))
                .add(tuple().field(string("139380")).field(string("AD2100")))
        );

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }

}
```


As you can see, there are several steps involved in creating a unit test for a pig script. It more or less behaves like a blackbox; you describe which data goes in and which data you expect to come out.

# Structure

Let's take the previous test class apart and explain its different components.

## Bootstrapping

```
	public class SimpleUnionTest {
	    private static final String PIG_SCRIPT = "src/test/resources/simpleUnion.pig";

	    @Test
	    @Category(TestCategories.PigTest.class)
	    public void testTextInput() throws Exception{
			...
	    }
	}
```

	This is mostly bookkeeping code. It creates the test class and the test method.  Maybe a bit special is the @Category annotation which is something new in Junit. It allows you to group your unit tests together. Since a PigUnit test will take a considerable amount of time to run, you probably don't want it to be a part of your standard build cycle.

Up to the Nifty part:

	We start by declaring our test runner. You can add the code of the script directly of you may choose to point to an existing file.

```
        // -- initialize the pig testing class
        NiftyPigTest test = new NiftyPigTest(PIG_SCRIPT);
```

	Next up we define the input data by declaring which alias should hold which data. The runner will modify the original Pig script and replace the original LOAD statements with new ones, pointing to temporary files holding the data you declare here. I told you it was nifty, didn't I.

```
        // -- indicate which data we want to use for which pig aliases
        String[] setA = {
                "139380;AD210",
                "139380;AD2100"
        };
        test.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO",
        };
        test.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);
```

	Once the inputs have been defined we can run the pig script to evaluate the expressions inside it. This will result in a lot of console output explaining what is being done.

```
        // -- actually execute the pig script
        test.execute();
```

	Last but not least we will validate the outcome of our test. We do this by mapping a DataSetValidator to a specific alias. This way we can validate the contents of any alias in the script.

```
	// -- validate the output using the DataSetValidator
	        test.validate(dataset("result").mode(DataSetValidator.ValidationMode.ByOrder).size(4)
	                .add(tuple().field(string("SOHO")).field(string("SOHO")))
	                .add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")))
	                .add(tuple().field(string("139380")).field(string("AD210")))
	                .add(tuple().field(string("139380")).field(string("AD2100")))
	        );
```

To make sure are result is valid we call the assert method assertTrue, to check if the generated report is valid.

```
Assert.assertTrue(report.isValid());
```

# Validations

There are several levels on which you can validate the outcome of a unit test. This section will go through all of them.

## DataSetValidator
Let's start with the validation of the resulting dataset itself. When defining a DataSetValidator we will indicate how much records we expect to retrieve and we will add one or more TupleValidator to validate if the tuples contain the right fields.

### Options
  -  Mode:
    - ValidationMode.Single:  In order to work the number of validators must be set to 1
    - ValidationMode.ByOrder: The results have to be in the order that the result is generated by the PIG script.
    - ValidationMode.BySelector: In the tuple you will need to implement select Builder which is a key mapping  for the defined array list of validation tuples

  - Size:  How many output records are expected

## FieldValidator
A field validator will only take the value of a single field into account. The following field validations are currently available:

  - any: Any value is allowed, even null.
  - anyButNull: Any value except null
  - isNotNull: Any value is allowed, as long as it isn't null
  - isNull: The value must be null
  - isString: The value must be a String
  - isNumber: The value must be an Integer or decimal number
  - string(expected): The value must be the same as the given string. This validator is case-sensitive
  - number: The value must be the same as the given number.
  - regex: The value must match the given regular expression.

# Extend

## Creating your own validator

There are times when you will need to create a new validator to implement some sort of validation routine. Don't be too alarmed about this, it is actually a lot easier then it seems.

All field validators are part of the FieldValidator class meaning the only thing you need to do is add another validator factory method to the class:

```
	public static FieldValidator any() {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
			FieldReport report = new FieldReport(fieldSequence);
                // -- Add your validation logic here.
			Return report;
		}
        };
	}
```

You may add your validation logic inside the validate method. The validate method receives the actuall fieldValue you want to validate, so you can start creating your tests.

If you want to parameterize your test (for example, to validate if a number is in a range) you may add these parameters to the validator constructor method:

```
	public static FieldValidator range(final Integer start, final Integer stop) {
        return new FieldValidator() {
            @Override
            public FieldValidator validate(Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

			  if (fieldValue == null) {
			    report.setMessage("The value was null while we expected it not to be null");
			    return report;
			 }
                Integer actual = Integer.parseInt(fieldValue.toString());
                if ( ! (start < actual && actual < stop)){
				report.setMessage ("The value was not ...");
				return report;
			}
            }
        };
	}
```

Beware that the validator construction method parameters have to be final.
