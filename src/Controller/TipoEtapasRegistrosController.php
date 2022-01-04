<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoEtapasRegistros Controller
 *
 * @property \App\Model\Table\TipoEtapasRegistrosTable $TipoEtapasRegistros
 * @method \App\Model\Entity\TipoEtapasRegistro[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoEtapasRegistrosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoEtapasRegistros = $this->paginate($this->TipoEtapasRegistros);

        $this->set(compact('tipoEtapasRegistros'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Etapas Registro id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoEtapasRegistro = $this->TipoEtapasRegistros->get($id, [
            'contain' => ['Usuarios'],
        ]);

        $this->set(compact('tipoEtapasRegistro'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoEtapasRegistro = $this->TipoEtapasRegistros->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoEtapasRegistro = $this->TipoEtapasRegistros->patchEntity($tipoEtapasRegistro, $this->request->getData());
            if ($this->TipoEtapasRegistros->save($tipoEtapasRegistro)) {
                $this->Flash->success(__('The tipo etapas registro has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo etapas registro could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoEtapasRegistro'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Etapas Registro id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoEtapasRegistro = $this->TipoEtapasRegistros->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoEtapasRegistro = $this->TipoEtapasRegistros->patchEntity($tipoEtapasRegistro, $this->request->getData());
            if ($this->TipoEtapasRegistros->save($tipoEtapasRegistro)) {
                $this->Flash->success(__('The tipo etapas registro has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo etapas registro could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoEtapasRegistro'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Etapas Registro id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoEtapasRegistro = $this->TipoEtapasRegistros->get($id);
        if ($this->TipoEtapasRegistros->delete($tipoEtapasRegistro)) {
            $this->Flash->success(__('The tipo etapas registro has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo etapas registro could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
