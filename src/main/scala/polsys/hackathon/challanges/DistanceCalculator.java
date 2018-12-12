package polsys.hackathon.challanges;

import java.util.Scanner;

abstract class Distance {
    protected int feet;
    protected float inches;

    abstract public void setFeetAndInches(int feet, float inches);
    abstract public int getFeet();
    abstract public float getInches();
    abstract String getDistanceComparison(Distance dist2);
}

class DistanceImplementation extends Distance{

    @Override
    public void setFeetAndInches(int feet, float inches) {
        this.feet=feet;
        this.inches=inches;
    }

    @Override
    public int getFeet() {
        return this.feet;
    }

    @Override
    public float getInches() {
        return inches;
    }

    @Override
    String getDistanceComparison(Distance dist2) {

        float f= this.getFeet()+(getInches()/12);
float f1=dist2.getFeet()+(getInches()/12);
        return f>f1?"F is greater":"f2 is greater";
    }
}
// Enter your code here.



public class DistanceCalculator {
    private static final Scanner scan = new Scanner(System.in);

    public static void main(String[] args) {
        Distance dist1 = new DistanceImplementation();
        Distance dist2 = new DistanceImplementation();

        int feet1 = 10;
        float inches1 = 1.6f;

        int feet2 = 2;
        float inches2 = 1.5f;

        dist1.setFeetAndInches(feet1, inches1);
        dist2.setFeetAndInches(feet2, inches2);

        System.out.println(dist1.getDistanceComparison(dist2));
    }
}