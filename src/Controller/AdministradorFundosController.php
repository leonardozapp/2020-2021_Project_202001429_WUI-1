<?php

declare(strict_types=1);

namespace App\Controller;

/**
 * AdministradorFundos Controller
 *
 * @property \App\Model\Table\AdministradorFundosTable $AdministradorFundos
 * @method \App\Model\Entity\AdministradorFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class AdministradorFundosController extends AppController {

	/**
	 * Index method
	 *
	 * @return \Cake\Http\Response|null|void Renders view
	 */
	public function index() {
		$administradorFundos = $this->paginate($this->AdministradorFundos);

		$this->set(compact('administradorFundos'));
	}

	/**
	 * View method
	 *
	 * @param string|null $id Administrador Fundo id.
	 * @return \Cake\Http\Response|null|void Renders view
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function view($id = null) {
		$administradorFundo = $this->AdministradorFundos->get($id, [
			'contain' => ['CadastroFundos'],
		]);

		$this->set(compact('administradorFundo'));
	}

	/**
	 * Add method
	 *
	 * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
	 */
	public function add() {
		$administradorFundo = $this->AdministradorFundos->newEmptyEntity();
		if ($this->request->is('post')) {
			$administradorFundo = $this->AdministradorFundos->patchEntity($administradorFundo, $this->request->getData());
			if ($this->AdministradorFundos->save($administradorFundo)) {
				$this->Flash->success(__('The administrador fundo has been saved.'));

				return $this->redirect(['action' => 'index']);
			}
			$this->Flash->error(__('The administrador fundo could not be saved. Please, try again.'));
		}
		$this->set(compact('administradorFundo'));
	}

	/**
	 * Edit method
	 *
	 * @param string|null $id Administrador Fundo id.
	 * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function edit($id = null) {
		$administradorFundo = $this->AdministradorFundos->get($id, [
			'contain' => [],
		]);
		if ($this->request->is(['patch', 'post', 'put'])) {
			$administradorFundo = $this->AdministradorFundos->patchEntity($administradorFundo, $this->request->getData());
			if ($this->AdministradorFundos->save($administradorFundo)) {
				$this->Flash->success(__('The administrador fundo has been saved.'));

				return $this->redirect(['action' => 'index']);
			}
			$this->Flash->error(__('The administrador fundo could not be saved. Please, try again.'));
		}
		$this->set(compact('administradorFundo'));
	}

	/**
	 * Delete method
	 *
	 * @param string|null $id Administrador Fundo id.
	 * @return \Cake\Http\Response|null|void Redirects to index.
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function delete($id = null) {
		$this->request->allowMethod(['post', 'delete']);
		$administradorFundo = $this->AdministradorFundos->get($id);
		if ($this->AdministradorFundos->delete($administradorFundo)) {
			$this->Flash->success(__('The administrador fundo has been deleted.'));
		} else {
			$this->Flash->error(__('The administrador fundo could not be deleted. Please, try again.'));
		}

		return $this->redirect(['action' => 'index']);
	}

	/*
	 * *******************************************************************************
	 */

	public function beforeFilter(\Cake\Event\EventInterface $event) {
		parent::beforeFilter($event);
		$session = $this->request->getSession();
		$conectado = $session->read('User.id') != null;
		if (!$conectado) {
			//$this->layout = 'ajax';
			$this->Flash->error(__('Você precisa estar logado para acessar a página solicitada. Você foi redirecionado à página principal.'));
			return $this->redirect(['controller' => 'Pages', 'action' => 'home']);
		}
	}

}
