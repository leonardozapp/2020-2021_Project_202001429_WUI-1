<?php

declare(strict_types=1);

namespace App\Controller;

use Cake\ORM\TableRegistry;

/**
 * Usuarios Controller
 *
 * @property \App\Model\Table\UsuariosTable $Usuarios
 * @method \App\Model\Entity\Usuario[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class UsuariosController_backup extends AppController {

	public function conta() {
		
	}

	public function perfilInvestidor() {
		
	}

	public function logout() {
		$session = $this->request->getSession();
		$name = $session->write('User.id', null);
		return $this->redirect('/');
	}

	/**
	 * Index method
	 *
	 * @return \Cake\Http\Response|null|void Renders view
	 */
	public function index() {
		$this->paginate = [
			'contain' => ['TipoPlanos', 'TipoEtapasRegistros'],
		];
		$usuarios = $this->paginate($this->Usuarios);

		$this->set(compact('usuarios'));
	}

	/**
	 * View method
	 *
	 * @param string|null $id Usuario id.
	 * @return \Cake\Http\Response|null|void Renders view
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function view($id = null) {
		$usuario = $this->Usuarios->get($id, [
			'contain' => ['TipoPlanos', 'TipoEtapasRegistros'],
		]);

		$this->set(compact('usuario'));
	}

	/**
	 * Add method
	 *
	 * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
	 */
	public function add() {
		$usuario = $this->Usuarios->newEmptyEntity();
		if ($this->request->is('post')) {
			$usuario = $this->Usuarios->patchEntity($usuario, $this->request->getData());
			if ($this->Usuarios->save($usuario)) {
				$this->Flash->success(__('The usuario has been saved.'));

				return $this->redirect(['action' => 'index']);
			}
			$this->Flash->error(__('The usuario could not be saved. Please, try again.'));
		}
		$tipoPlanos = $this->Usuarios->TipoPlanos->find('list', ['limit' => 200]);
		$tipoEtapasRegistros = $this->Usuarios->TipoEtapasRegistros->find('list', ['limit' => 200]);
		$this->set(compact('usuario', 'tipoPlanos', 'tipoEtapasRegistros'));
	}

	/**
	 * Edit method
	 *
	 * @param string|null $id Usuario id.
	 * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function edit($id = null) {
		$usuario = $this->Usuarios->get($id, [
			'contain' => [],
		]);
		if ($this->request->is(['patch', 'post', 'put'])) {
			$usuario = $this->Usuarios->patchEntity($usuario, $this->request->getData());
			if ($this->Usuarios->save($usuario)) {
				$this->Flash->success(__('The usuario has been saved.'));

				return $this->redirect(['action' => 'index']);
			}
			$this->Flash->error(__('The usuario could not be saved. Please, try again.'));
		}
		$tipoPlanos = $this->Usuarios->TipoPlanos->find('list', ['limit' => 200]);
		$tipoEtapasRegistros = $this->Usuarios->TipoEtapasRegistros->find('list', ['limit' => 200]);
		$this->set(compact('usuario', 'tipoPlanos', 'tipoEtapasRegistros'));
	}

	/**
	 * Delete method
	 *
	 * @param string|null $id Usuario id.
	 * @return \Cake\Http\Response|null|void Redirects to index.
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function delete($id = null) {
		$this->request->allowMethod(['post', 'delete']);
		$usuario = $this->Usuarios->get($id);
		if ($this->Usuarios->delete($usuario)) {
			$this->Flash->success(__('The usuario has been deleted.'));
		} else {
			$this->Flash->error(__('The usuario could not be deleted. Please, try again.'));
		}

		return $this->redirect(['action' => 'index']);
	}

	/**
	 * Register method
	 *
	 * @param string|null $id Usuario id.
	 * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
	 * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
	 */
	public function register() {
		$usuario = $this->Usuarios->newEmptyEntity();
		if ($this->request->is('post')) {
			$usuario = $this->Usuarios->patchEntity($usuario, $this->request->getData());
			// completa campos inexistentes no formulário
			$usuario->coment = "";
			$usuario->dt_reg = date('y-m-d');
			$usuario->tipo_etapas_registro_id = 1;
			$usuario->tipo_plano_id = 1;
			//
			if ($this->Usuarios->save($usuario)) {
				$this->Flash->success(__('The usuario has been saved.'));

				return $this->redirect(['controller' => 'Usuarios', 'action' => 'login', $usuario->get('id'), true]);
			}
			$this->Flash->error(__('The usuario could not be saved. Please, try again.'));
		}
		$tipoPlanos = $this->Usuarios->TipoPlanos->find('list', ['limit' => 200]);
		$tipoEtapasRegistros = $this->Usuarios->TipoEtapasRegistros->find('list', ['limit' => 200]);
		$this->set(compact('usuario', 'tipoPlanos', 'tipoEtapasRegistros'));
	}

	public function login($id = null, $recemRegistrado = null) {
		//$usuario = $this->Usuarios->newEmptyEntity();
		if ($this->request->is('post')) {
			$dadosLogin = $this->request->getData(); //$this->Usuarios->patchEntity($usuario, $this->request->getData());
			$query = $this->Usuarios->find()->where('cpf=' . $dadosLogin['cpf']);
			$usuario = $query->first();
			if ($usuario != null) {
				if ($dadosLogin['senha'] == $usuario['senha']) { //verifica que login e senha são válidos
					$session = $this->request->getSession();
					//$session->write('User.name', $usuario['nome']);
					$words = explode(' ', $usuario['nome']);
					$session->write('User.firstname', $words[0]);
					$session->write('User.id', $usuario['id']);
					$this->
					
					$session->write('User.permissions', $usuario['id']);
					return $this->redirect('/');
				}
			}
			$this->Flash->error(__('Login incorreto. Por favor, tente novamente.'));
		}
		//if ($id != null) {
		//	$query = $this->Usuarios->find()->where('id=' . $id);
		//	$usuario = $query->firstOrFail();
		//}
		//$this->set(compact('usuario', 'recemRegistrado'));
	}

}
