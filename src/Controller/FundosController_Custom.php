<?php

declare(strict_types=1);

namespace App\Controller;

use Cake\ORM\TableRegistry;
use App\Form\FundosFiltroForm;

/**
 * Fundos Controller
 *
 */
class FundosController extends AppController {

// In a controller or table method.

	public $paginate = array(
		'maxLimit' => 20, //Registros por página
//		'limit' => 100, //Registros por consulta
		'paramType' => 'querystring' //Esta linha analisa o parâmetro fornecido pelo link.
	);

	public function index() {
		$this->redirect('/Fundos/busca');
	}

	/**
	 * Busca method
	 *
	 * @return \Cake\Http\Response|null|void Renders view
	 */
	public function busca() {
		//
		// filtro
		$formFiltro = new FundosFiltroForm();
		$formFiltro->setErrors(["email" => ["_required" => "Your email is required"]]);
		if ($this->request->is('post')) {
			if ($formFiltro->execute($this->request->getData())) {
				$this->Flash->success('We will get back to you soon.');
			} else {
				$this->Flash->error('There was a problem submitting your form.');
			}
		}
		if ($this->request->is('get')) {
			$formFiltro->setData([
				'nome' => 'John Doe',
				'tipo' => 'john.doe@example.com'
			]);
		}
		$this->set('filtroForm', $formFiltro);
		//
		$ClassesFundos = TableRegistry::getTableLocator()->get('TipoClasseFundos');
		$query = $ClassesFundos->find();
		$classeFundos = $this->paginate($query);
		$this->set(compact('classeFundos'));
		//
		$ClassesAnbima = TableRegistry::getTableLocator()->get('TipoAnbimaClasses');
		$query = $ClassesAnbima->find();
		$classeAnbima = $this->paginate($query);
		$this->set(compact('classeAnbima'));
		//
		// lista
		$CnpjFundos = TableRegistry::getTableLocator()->get('CnpjFundos');
		$query = $CnpjFundos->find()
				//->contain(['CadastroFundos' => ['TipoClasseFundos', 'TipoRentabilidadeFundos', 'AdministradorFundos'], 'DocExtratosFundos' => ['sort' => ['DocExtratosFundos.DT_COMPTC' => 'DESC']], 'SituacaoFundos' => ['sort' => ['SituacaoFundos.DT_INI_SIT' => 'DESC'], 'TipoSituacaoFundos']])
				->contain(['CadastroFundos' => ['sort' => ['CadastroFundos.DT_REG_CVM ' => 'DESC'], 'TipoClasseFundos', 'TipoRentabilidadeFundos', 'AdministradorFundos']])
				->contain(['DocExtratosFundos' => ['sort' => ['DocExtratosFundos.DT_COMPTC' => 'DESC']]])
				->contain(['SituacaoFundos' => ['sort' => ['SituacaoFundos.DT_INI_SIT' => 'DESC'], 'TipoSituacaoFundos']])
				->notMatching('CancelamentoFundos')
				->where([['CnpjFundos.DENOM_SOCIAL != ' => ' --DESCONHECIDO--']]) // e atualmente tem pelo menos 1000 cotistas (como fazer isso?)
		//->matching('SituacaoFundos', function ($q) {return $q->where(['SituacaoFundos.tipo_situacao_fundo_id' => 1]);	})
		//->andWhere('SituacaoFundos.tipo_situacao_fundo_id = 1')
		//->order('SituacaoFundos.DT_INI_SIT desc')
		;
		// aplica filtros customizados
		//$query->find()->where(['SituacaoFundos.tipo_situacao_fundo_id >= 0']);
		//
		$cnpjFundos = $this->paginate($query);
		$this->set(compact('cnpjFundos'));
	}

	/**
	 * View method
	 *
	 * @param string|null $id Cnpj Fundo id.
	 * @return \Cake\Http\Response|null|void Renders view
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function view($id = null) {
		$CnpjFundos = TableRegistry::getTableLocator()->get('CnpjFundos');
		$cnpjFundo = $CnpjFundos->get($id, [
			'contain' => ['DocInfDiarioFundos' => ['sort' => ['DocInfDiarioFundos.DT_COMPTC' => 'ASC']], 'CancelamentoFundos', 'CadastroFundos' => ['sort' => ['CadastroFundos.DT_REG_CVM ' => 'DESC'], 'TipoClasseFundos', 'TipoRentabilidadeFundos', 'GestorFundos', 'AdministradorFundos'],
				'DocExtratosFundos' => ['TipoAnbimaClasses'],
				'SituacaoFundos' => ['sort' => ['SituacaoFundos.DT_INI_SIT' => 'DESC'], 'TipoSituacaoFundos']],
		]);
		$this->set(compact('cnpjFundo'));
		//
		$IndicadoresFundos = TableRegistry::getTableLocator()->get('IndicadoresFundos');
		$indicadores = $IndicadoresFundos->find()->where(['cnpj_fundo_id' => $id]);
		$this->set(compact('indicadores'));
		//var_dump($indicadoes);
		//exit();
		//
		// cálculo antigo das rentabilidades e volatilidades. Não precisa mais. Está pré-calculado no BD
		/*
		  $rentabVolat = array();
		  $valorAnterior = $cnpjFundo['doc_inf_diario_fundos'][0]['VL_QUOTA'];
		  $primeiroValor = $valorAnterior;
		  $maxValDrawdown = 0.0;
		  $n = 1;
		  $sumDiffQuad = 0.0;
		  foreach ($cnpjFundo['doc_inf_diario_fundos'] as $diario) {
		  $valorAtual = $diario['VL_QUOTA'];
		  if ($valorAtual > $maxValDrawdown) {
		  $maxValDrawdown = $valorAtual;
		  }
		  $rentabilidadeDiaria = ($valorAtual - $valorAnterior) / $valorAnterior; //($valorAtual / $valorAnterior - 1.0);
		  $rentabilidadeAcumuladaAtual = ($valorAtual - $primeiroValor) / $primeiroValor;  //($valorAtual / $primeiroValor - 1.0);
		  $sumDiffQuad += pow($rentabilidadeDiaria - $rentabilidadeAcumuladaAtual, 2);
		  $volatilidade = sqrt($sumDiffQuad/$n);
		  if ($valorAtual < $maxValDrawdown) {
		  $drawdown = $valorAtual / $maxValDrawdown - 1.0;
		  } else {
		  $drawdown = 0.0;
		  }
		  $rentabVolat[] = ['data' => $diario['DT_COMPTC']->format('d/m/Y'), 'rentabilidade' => $rentabilidadeDiaria, 'volatilidade' => $volatilidade, 'drawdown' => $drawdown, 'rentabilidadeAcumulada' => $rentabilidadeAcumuladaAtual];
		  //atualiza
		  $valorAnterior = $valorAtual;
		  $n++;
		  if($n>5) {
		  $sumDiffQuad=0.0;
		  $n=1;
		  }
		  }
		  $this->set(compact('rentabVolat'));
		 */

		/*
		  //
		  // Cálculo das rentabilidades mensais
		  $valorAnterior = $cnpjFundo['doc_inf_diario_fundos'][0]['VL_QUOTA'];
		  $valorInicial = $valorAnterior;
		  $mesAnterior = $cnpjFundo['doc_inf_diario_fundos'][0]['DT_COMPTC']->format('m/y');
		  $rentabsDiariasMesAtual = array();
		  $sumRentabsDiariasMesAtual = 0.0;
		  $rentabVolat = array();
		  $rentabMediaAcumulada = 0.0;
		  $rentabAcumuladaAnterior = 0.0;
		  foreach ($cnpjFundo['doc_inf_diario_fundos'] as $diario) {
		  $mesAtual = $diario['DT_COMPTC']->format('m/y');
		  $valorAtual = $diario['VL_QUOTA'];
		  if ($mesAnterior == $mesAtual) {
		  // acumula valores
		  $rentabDia = ($valorAtual / $valorAnterior - 1.0);
		  $rentabsDiariasMesAtual[] = $rentabDia;
		  $sumRentabsDiariasMesAtual += $rentabDia;
		  //echo ($mesAtual . '  Valor:' . $diario['VL_QUOTA'] . ' ; Rentab: ' . $rentabDia . '<br/>');
		  } else {
		  // calcula estatísticas
		  //echo('Resultados do mês '.$mesAtual.' = ');
		  $n = count($rentabsDiariasMesAtual);
		  $rentabMedia = $sumRentabsDiariasMesAtual / $n;
		  $rentabMediaAcumulada += $rentabMedia;
		  $rentabAcumuladaAtual = ($valorAtual / $valorInicial - 1.0);
		  if ($rentabAcumuladaAtual < $rentabAcumuladaAnterior) {
		  $drawdown = 1.0 - $rentabAcumuladaAtual / $rentabAcumuladaAnterior;
		  } else {
		  $drawdown = 0.0;
		  }
		  $sumDifQuad = 0.0;
		  for ($i = 0; $i < $n; $i++) {
		  $sumDifQuad += pow($rentabsDiariasMesAtual[$i] - $rentabMedia, 2);
		  }
		  $desvioPadrao = sqrt($sumDifQuad / $n);
		  $rentabVolat[] = ['mes' => $mesAtual, 'rentabilidade' => $rentabMedia, 'desvio' => $desvioPadrao, 'drawdown' => $drawdown, 'rentabMediaAcumulada' => $rentabMediaAcumulada, 'rentabilidadeAcumulada' => $rentabAcumuladaAtual];
		  //echo('Dias: ' . $n . ' ; Rentabilidade mensal: ' . $rentabMedia . ' ; Desvio-padrão: ' . $desvioPadrao);
		  //echo('<br/>');
		  // limpa para o próximo mês
		  $rentabsDiariasMesAtual = array();
		  $sumRentabsDiariasMesAtual = 0.0;
		  $mesAnterior = $mesAtual;
		  $rentabAcumuladaAnterior = $rentabAcumuladaAtual;
		  }
		  $valorAnterior = $valorAtual;
		  }

		  $this->set(compact('rentabVolat'));
		 * 
		 */
	}

	/**
	 * Comparacao method
	 *
	 * @return \Cake\Http\Response|null|void Renders view
	 */
	public function comparacao() {
		$RetornoRiscoFundos = TableRegistry::getTableLocator()->get('RetornoRiscoFundos');
		$this->paginate = [
			'contain' => ['CnpjFundos' => ['CadastroFundos' => ['TipoClasseFundos']]],
		];
		$retornoRiscoFundos = $this->paginate($RetornoRiscoFundos);
		$this->set(compact('retornoRiscoFundos

		'));
	}

	/**
	 * Comparacao method
	 *
	 * @return \Cake\Http\Response|null|void Renders view
	 */
	public function indicadores() {
		
	}

}
