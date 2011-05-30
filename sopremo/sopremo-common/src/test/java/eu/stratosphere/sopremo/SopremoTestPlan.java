package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.testing.TestPairs;
import eu.stratosphere.pact.testing.TestPlan;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.base.DataType;
import eu.stratosphere.sopremo.base.Sink;
import eu.stratosphere.sopremo.base.Source;

public class SopremoTestPlan {
	public static class MockupSource extends Source {

		private int index;

		public MockupSource(int index) {
			super(DataType.HDFS, "mockup-input" + index);
			this.index = index;
		}

		@Override
		public PactModule asPactModule(EvaluationContext context) {
			PactModule pactModule = new PactModule(0, 1);
			DataSourceContract contract = TestPlan.createDefaultSource(this.getInputName());
			pactModule.getOutput(0).setInput(contract);
//			pactModule.setInput(0, contract);
			return pactModule;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			MockupSource other = (MockupSource) obj;
			return this.index == other.index;
		}

	}

	public static class MockupSink extends Sink {
		private int index;

		public MockupSink(int index) {
			super(DataType.ADHOC, "mockup-output" + index, null);
			this.index = index;
		}

		@Override
		public PactModule asPactModule(EvaluationContext context) {
			PactModule pactModule = new PactModule(1, 0);
			DataSinkContract contract = TestPlan.createDefaultSink(this.getOutputName());
			contract.setInput(pactModule.getInput(0));
			pactModule.addInternalOutput(contract);
			return pactModule;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = super.hashCode();
			result = prime * result + this.index;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (this.getClass() != obj.getClass())
				return false;
			MockupSink other = (MockupSink) obj;
			return this.index == other.index;
		}
	}

	public static class Input {
		private int index;

		private Operator operator;

		private TestPairs<PactNull, PactJsonObject> input = new TestPairs<PactNull, PactJsonObject>();

		public Input(int index) {
			this.index = index;
			this.operator = new MockupSource(index);
		}

		public int getIndex() {
			return this.index;
		}

		public Operator getOperator() {
			return this.operator;
		}

		public Input add(PactJsonObject object) {
			this.input.add(new KeyValuePair<PactNull, PactJsonObject>(PactNull.getInstance(), object));
			return this;
		}

		public void setOperator(Operator operator) {
			if (operator == null)
				throw new NullPointerException("operator must not be null");

			this.operator = operator;
		}

		public void prepare(TestPlan testPlan) {
			if (this.operator instanceof MockupSource)
				testPlan.getInput(this.index).add(this.input);
		}
	}

	public static class Output {
		private int index;

		private Operator operator;

		private TestPairs<PactNull, PactJsonObject> expected = new TestPairs<PactNull, PactJsonObject>();

		public Output(int index) {
			this.index = index;
			this.operator = new MockupSink(index);
		}

		public int getIndex() {
			return this.index;
		}

		public Operator getOperator() {
			return this.operator;
		}

		public Output add(PactJsonObject object) {
			this.expected.add(new KeyValuePair<PactNull, PactJsonObject>(PactNull.getInstance(), object));
			return this;
		}

		public void setOperator(Operator operator) {
			if (operator == null)
				throw new NullPointerException("operator must not be null");

			this.operator = operator;
		}

		public void prepare(TestPlan testPlan) {
			if (this.operator instanceof MockupSink)
				testPlan.getExpectedOutput(this.index).add((TestPairs<PactNull, PactJsonObject>) this.expected);
		}
	}

	private Input[] inputs;

	private Output[] outputs;

	public SopremoTestPlan(int numInputs, int numOutputs) {
		this.inputs = new Input[numInputs];
		for (int index = 0; index < numInputs; index++)
			this.inputs[index] = new Input(index);
		this.outputs = new Output[numOutputs];
		for (int index = 0; index < numOutputs; index++)
			this.outputs[index] = new Output(index);
	}

	public Input getInput(int index) {
		return this.inputs[index];
	}

	public Operator getInputOperator(int index) {
		return this.getInput(index).getOperator();
	}

	public Operator[] getInputOperators(int from, int to) {
		Operator[] operators = new Operator[to - from];
		for (int index = 0; index < operators.length; index++)
			operators[index] = this.getInputOperator(from + index);
		return operators;
	}

	public Operator[] getOutputOperators(int from, int to) {
		Operator[] operators = new Operator[to - from];
		for (int index = 0; index < operators.length; index++)
			operators[index] = this.getOutputOperator(from + index);
		return operators;
	}

	public Output getExpectedOutput(int index) {
		return this.outputs[index];
	}

	public Operator getOutputOperator(int index) {
		return this.getExpectedOutput(index).getOperator();
	}

	public void setOutputOperator(int index, Operator operator) {
		this.outputs[index].setOperator(operator);
	}

	public void setInputOperator(int index, Operator operator) {
		this.inputs[index].setOperator(operator);
	}

	public void run() {
		SopremoPlan sopremoPlan = new SopremoPlan(this.getOutputOperators(0, this.outputs.length));
		TestPlan testPlan = new TestPlan(sopremoPlan.assemblePact());
		for (Input input : this.inputs)
			input.prepare(testPlan);
		for (Output output : this.outputs)
			output.prepare(testPlan);
		testPlan.run();
	}
}
