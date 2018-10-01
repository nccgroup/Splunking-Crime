#!/usr/bin/env python

import json

import numpy as np
import pandas as pd
from pytest import raises
from sklearn.cluster import Birch
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import GenericUnivariateSelect
from sklearn.kernel_ridge import KernelRidge
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import Ridge
from sklearn.naive_bayes import BernoulliNB
from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from sklearn.svm.classes import OneClassSVM
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import DecisionTreeRegressor

from algos.BernoulliNB import BernoulliNB as BernoulliNB_Algo
from algos.Birch import Birch as Birch_algo
from algos.DecisionTreeClassifier import DecisionTreeClassifier as DecisionTreeClassifierAlgo
from algos.DecisionTreeRegressor import DecisionTreeRegressor as DecisionTreeRegressorAlgo
from algos.ElasticNet import ElasticNet as ElasticNetAlgo
from algos.FieldSelector import FieldSelector
from algos.GaussianNB import GaussianNB as GaussianNB_Algo
from algos.KMeans import KMeans as KMeansAlgo
from algos.KernelRidge import KernelRidge as KernelRidgeAlgo
from algos.Lasso import Lasso as LassoAlgo
from algos.LinearRegression import LinearRegression as LinearRegressionAlgo
from algos.LogisticRegression import LogisticRegression as LogisticRegressionAlgo
from algos.OneClassSVM import OneClassSVM as OneClassSVM_Algo
from algos.PCA import PCA as PCA_algo
from algos.RandomForestClassifier import RandomForestClassifier as RandomForestClassifierAlgo
from algos.RandomForestRegressor import RandomForestRegressor as RandomForestRegressorAlgo
from algos.Ridge import Ridge as RidgeAlgo
from algos.SVM import SVM
from algos.StandardScaler import StandardScaler as StandardScalerAlgo
from algos.TFIDF import TFIDF
from codec import MLSPLEncoder, MLSPLDecoder


class TestCodec():
    def ndarray_util(self, a):
        j = json.dumps(a, cls=MLSPLEncoder)
        o = json.loads(j, cls=MLSPLDecoder)
        assert (a == o).all()

        # Alternative syntax.
        j = MLSPLEncoder().encode(a)
        o = MLSPLDecoder().decode(j)
        assert (a == o).all()

    def clusterer_util(self, clusterer):
        model_orig = clusterer()
        X = np.random.randn(30, 5)
        model_orig.fit(X)
        j = json.dumps(model_orig, cls=MLSPLEncoder)
        model_decoded = json.loads(j, cls=MLSPLDecoder)
        array = X[0].reshape(1, -1)
        original = model_orig.predict(array)
        decoded = model_decoded.predict(array)
        np.testing.assert_array_equal(original, decoded)

    def transformer_util(self, transformer):
        model_orig = transformer()
        X = np.random.randn(30, 5)
        model_orig.fit(X)
        j = json.dumps(model_orig, cls=MLSPLEncoder)
        model_decoded = json.loads(j, cls=MLSPLDecoder)
        array = X[0].reshape(1, -1)
        original = model_orig.transform(array)
        decoded = model_decoded.transform(array)
        np.testing.assert_array_equal(original, decoded)

    def estimator_util(self, estimator, X, y):
        model_orig = estimator()
        model_orig.fit(X, y)
        j = json.dumps(model_orig, cls=MLSPLEncoder)
        model_decoded = json.loads(j, cls=MLSPLDecoder)
        array = np.array(X[0]).reshape(1, -1)
        original = model_orig.predict(array)
        decoded = model_decoded.predict(array)
        np.testing.assert_array_equal(original, decoded)

    def selector_util(self, estimator, X, y):
        model_orig = estimator(mode='k_best', param=1)
        model_orig.fit(X, y)
        j = json.dumps(model_orig, cls=MLSPLEncoder)
        model_decoded = json.loads(j, cls=MLSPLDecoder)
        array = np.array(X[0]).reshape(1, -1)
        original = model_orig.transform(array)
        decoded = model_decoded.transform(array)
        np.testing.assert_array_equal(original, decoded)

    def regressor_util(self, estimator):
        self.estimator_util(estimator, [[1, 2], [3, 4]], [5, 6])

    def classifier_util(self, estimator):
        self.estimator_util(estimator, [[1, 2], [3, 4]], ['red', 'blue'])
        self.estimator_util(estimator, [[1, 2], [3, 4]], ['world', u'hello\u0300'])
        self.estimator_util(estimator, [[1, 2], [3, 4]], [u'hello\u0300', 'world'])
        self.estimator_util(estimator, [[1, 2], [3, 4]], [5, 6])

    def pod_util(self, obj_orig):
        j = json.dumps(obj_orig, cls=MLSPLEncoder)
        obj_decoded = json.loads(j, cls=MLSPLDecoder)
        assert obj_orig == obj_decoded

    def test_ndarray(self):
        self.ndarray_util(np.array([[1, 2], [3, 4]]))
        self.ndarray_util(np.array([1.5, 2.5, 3.5]))
        self.ndarray_util(np.array([True, False, True, False]))
        self.ndarray_util(np.array(['hello', 'world']))
        self.ndarray_util(np.array([u'hello\u0300', u'w\u0300rld']))

    def test_PCA(self):
        PCA_algo.register_codecs()
        self.transformer_util(PCA)

    def test_KMeans(self):
        KMeansAlgo.register_codecs()
        self.clusterer_util(KMeans)

    def test_Birch(self):
        Birch_algo.register_codecs()
        self.clusterer_util(Birch)

    def test_DecisionTreeClassifier(self):
        DecisionTreeClassifierAlgo.register_codecs()
        self.classifier_util(DecisionTreeClassifier)

    def test_RandomForestClassifier(self):
        RandomForestClassifierAlgo.register_codecs()
        self.classifier_util(RandomForestClassifier)

    def test_GaussianNB(self):
        GaussianNB_Algo.register_codecs()
        self.classifier_util(GaussianNB)

    def test_BernoulliNB(self):
        BernoulliNB_Algo.register_codecs()
        self.classifier_util(BernoulliNB)

    def test_DecisionTreeRegressor(self):
        DecisionTreeRegressorAlgo.register_codecs()
        self.regressor_util(DecisionTreeRegressor)

    def test_RandomForestRegressor(self):
        RandomForestRegressorAlgo.register_codecs()
        self.regressor_util(RandomForestRegressor)

    def test_Lasso(self):
        LassoAlgo.register_codecs()
        self.regressor_util(Lasso)

    def test_ElasticNet(self):
        ElasticNetAlgo.register_codecs()
        self.regressor_util(ElasticNet)

    def test_Ridge(self):
        RidgeAlgo.register_codecs()
        self.regressor_util(Ridge)

    def test_LinearRegression(self):
        LinearRegressionAlgo.register_codecs()
        self.regressor_util(LinearRegression)

    def test_LogisticRegression(self):
        LogisticRegressionAlgo.register_codecs()
        self.regressor_util(LogisticRegression)

    def test_GenericUnivariateSelect(self):
        FieldSelector.register_codecs()
        self.selector_util(GenericUnivariateSelect, [[1, 2], [3, 4]], ['red', 'blue'])
        self.selector_util(GenericUnivariateSelect, [[1, 2], [3, 4]], ['world', u'hello\u0300'])
        self.selector_util(GenericUnivariateSelect, [[1, 2], [3, 4]], [u'hello\u0300', 'world'])
        self.selector_util(GenericUnivariateSelect, [[1, 2], [3, 4]], [5, 6])

    def test_SVC(self):
        SVM.register_codecs()
        self.regressor_util(SVC)

    def test_OneClassSVM(self):
        OneClassSVM_Algo.register_codecs()
        self.clusterer_util(OneClassSVM)

    def test_TFIDF(self):
        TFIDF.register_codecs()
        X = ['the quick brown fox jumps over the lazy dog']
        m = TfidfVectorizer()
        m.fit(X)
        j = json.dumps(m, cls=MLSPLEncoder)
        o = json.loads(j, cls=MLSPLDecoder)
        assert (m.transform(X) == o.transform(X)).toarray().all()

    def test_StandardScaler(self):
        StandardScalerAlgo.register_codecs()
        self.transformer_util(StandardScaler)

    def test_KernelRidge(self):
        KernelRidgeAlgo.register_codecs()
        self.regressor_util(KernelRidge)

    def test_pod(self):
        self.pod_util({"foo": 1})
        self.pod_util({u"foo": 1})
        self.pod_util({"foo": u"bar"})
        self.pod_util({u"foo": u"bar"})
        self.pod_util([1, 2, 3])
        self.pod_util([1.5, 2.5, 3.5])
        self.pod_util([{"foo": "bar"}])
        self.pod_util({"foo": ["bar", 5]})

        # JSON objects always have a string key
        with raises(AssertionError):
            self.pod_util({1: "foo"})

    def test_np_builtin(self):
        self.pod_util(np.int64(42))
        self.pod_util(np.int32(42))
        self.pod_util(np.int16(42))
        self.pod_util(np.int8(42))
        self.pod_util(np.uint64(42))
        self.pod_util(np.uint32(42))
        self.pod_util(np.uint16(42))
        self.pod_util(np.uint8(42))
        self.pod_util(np.float16(42))
        self.pod_util(np.float32(42))
        self.pod_util(np.float64(42))
        # self.pod_util(np.float128(42))
        self.pod_util(np.complex64(42))
        self.pod_util(np.complex128(42))
        # self.pod_util(np.complex256(42))

    def test_pandas(self):
        df = pd.DataFrame({'a': range(10), 'b': np.arange(10) * 0.1})
        j = json.dumps(df, cls=MLSPLEncoder)
        o = json.loads(j, cls=MLSPLDecoder)
        assert (df == o).all().all()

