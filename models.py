import pandas as pd
import numpy as np
from sklearn.model_selection import KFold  # For K-fold cross validation
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics


def main():

    train = pd.read_csv("train.csv");
    test = pd.read_csv("test.csv");

    decisionTree(train, test);
    randomForest(train, test);


def decisionTree(train, test):
    feature_cols = ['diffrenceWins', 'potential_difference', 'speed_difference']  # adds also 'speed_difference',
    X_train = train[feature_cols]  # Features
    y_train = train['home_status']  # Target variable

    X_test = test[feature_cols]  # Features
    y_test = test['home_status']  # Target variable

    # Create Decision Tree classifer object
    clf = tree.DecisionTreeClassifier(max_depth=20)

    # Train Decision Tree Classifer
    clf = clf.fit(X_train, y_train)

    # Predict the response for test dataset
    y_pred_test = clf.predict(X_test)
    y_pred_train = clf.predict(X_train)

    # Prints train' accuracy
    accuracy = metrics.accuracy_score(y_pred_train, y_train)
    print("Training accuracy : %s" % "{0:.3%}".format(accuracy))

    # Model test' accuracy, (how often is the classifier correct)
    print("DecisionTreeClassifier accuracy:", metrics.accuracy_score(y_test, y_pred_test) * 100, "%")

    # Create a series with features' importance:
    featimp = pd.Series(clf.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print(featimp)
    print()


def randomForest(train, test):
    feature_cols = ['diffrenceWins', 'overall_rating_difference', 'potential_difference', 'chance_creations_difference',
                    'speed_difference', 'pass_difference', 'defence_pressure_difference', 'defence_aggression_difference', 'diffrenceGoals']  # adds also 'speed_difference',
    X_train = train[feature_cols]  # Features
    y_train = train['home_status']  # Target variable

    X_test = test[feature_cols]  # Features
    y_test = test['home_status']  # Target variable

    # Create a Gaussian Classifier
    clf = RandomForestClassifier(n_estimators=100, max_depth=14)

    # Train the model using the training sets y_pred=clf.predict(X_test)
    clf.fit(X_train, y_train)

    y_pred_test = clf.predict(X_test)
    y_pred_train = clf.predict(X_train)

    # Prints train' accuracy
    accuracy = metrics.accuracy_score(y_pred_train, y_train)
    print("Training accuracy : %s" % "{0:.3%}".format(accuracy))

    # Model test' accuracy, how often is the classifier correct?
    print("RandomForestClassifier accuracy:", metrics.accuracy_score(y_test, y_pred_test) * 100, "%")

    # Create a series with features' importance:
    featimp = pd.Series(clf.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print(featimp)


main();